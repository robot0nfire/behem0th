#
# Copyright (c) 2016 Christoph Heiss <me@christoph-heiss.me>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

import os
import json
import struct
import threading
import socket
import queue
import tempfile
import base64
import select
from behem0th import utils, log

BLOCK_SIZE = 4096


class Route:
	def handle(self, data, request):
		raise NotImplementedError


	def send(self, data):
		self.handler.send(self.route_name, data)


class FilelistRoute(Route):
	def handle(self, data, request):
		if request.is_client:
			request.client._filelist = data
			request.client._rlock.release()
		else:
			files, events = request.client._merge_filelist(data)
			with request.client._rlock:
				self.send(request.client._filelist)

			for e in events:
				request.queue_event(e)
			for f in files:
				request.queue_file(f[0], f[1])


"""
	{
		"action": "<action>",
		"path": "<relpath-to-file>"
	}
	<action> can be either 'receive' or 'send'

	Payload are base64 encoded chunks (BLOCK_SIZE bytes)
"""
class FileRoute(Route):
	def handle(self, data, request):
		action = data['action']
		path = data['path']

		if action == 'receive':
			tmpf = tempfile.NamedTemporaryFile(delete=False)

			buffer = b''
			for chunk in request.recv():
				buffer += chunk

				if len(buffer) >= BLOCK_SIZE:
					tmpf.write(base64.b64decode(buffer[:BLOCK_SIZE]))
					buffer = buffer[:BLOCK_SIZE]
			tmpf.write(base64.b64decode(buffer))

			tmpf.close()

			# watchdog reports a file-deleted and a file-created event, so ignore both.
			request.client._ignore_next_fsevent(path)
			request.client._ignore_next_fsevent(path)
			os.rename(tmpf.name, request.client._abspath(path))

			request.client._update_metadata(path)
			request.client._event_handler._dispatch(
				'received', request.client, path, 'file'
			)

		elif action == 'send':
			request.queue_file('send', path)

		else:
			log.warn('FileRoute: Unknown action \'{0}\', igoring.', action)

		# If we are the 'server', we also need to distribute all file request
		# to all other clients.
		if not request.is_client:
			action = 'send' if action == 'receive' else 'request'
			request.client._run_on_peers('queue_file', request, action, path)


"""
	{
		"type": "<type>",
		"path": "<relpath-to-file>"
	}
	<type> can be one of 'file-created', 'file-deleted', 'file-moved'

"""
class EventRoute(Route):
	def handle(self, data, request):
		f_type, event = data['type'].split('-')
		path = data['path']
		abspath = request.client._abspath(path)

		request.client._ignore_next_fsevent(path)

		# TODO: factor out common code with Client._handle_fsevent() and Client._merge_filelist()
		if event == 'created':
			# create the file/directory
			if f_type == 'file':
				open(abspath, 'a').close()
			else:
				os.mkdir(abspath, 0o755)

			request.client._add_to_filelist(path, f_type)

		elif event == 'deleted':
			request.client._remove_from_filelist(path)
			os.remove(abspath)

		elif event == 'moved':
			request.client._remove_from_filelist(path)
			os.rename(abspath, data['dest'])
			request.client._add_to_filelist(data['dest'], f_type)

		else:
			log.warn('EventRoute: Unknown event {0}', data)

		# For rationale, see FileRoute.handle()
		if not request.is_client:
			request.client._run_on_peers('queue_event', request, data)

ROUTES = {
	'filelist': FilelistRoute(),
	'file': FileRoute(),
	'event': EventRoute()
}


"""
	behem0th's protocol is completely text-based, using utf-8 encoding and
	encoded in JSON for easy parsing.

	A request usually looks like this:
		{ "route": "<route-name>", "data": "<data>" }

	'data' holds additional data which is then passed to the route.
	There is no special format designed for 'data' and is specific to each route.

	After each request there is a newline to separate them. (think of HTTP)

	If a route needs to transfer additional data (a 'payload'), it has to send them
	in a text-based format, e.g. base-64 encoding for binary data.

	After the payload, if any, there has to be another newline to separate it from
	the next request.
"""
class RequestHandler(threading.Thread):
	req_handler_num = 0

	def __init__(self, **kwargs):
		super().__init__()
		self.daemon = True
		self.sync_queue = queue.Queue()
		self.routes = {}
		self.recvbuf = b''

		RequestHandler.req_handler_num += 1
		self.name = "request-handler-{0}".format(RequestHandler.req_handler_num)
		for key, value in kwargs.items():
			setattr(self, key, value)

		with self.client._rlock:
			self.client._peers.append(self)

		self.sock.setblocking(0)
		self.is_client = bool(self.client._sock)

		for name, route in ROUTES.items():
			route.route_name = name
			route.handler = self
			self.routes[name] = route


	def setup(self):
		log.info('Connected to {0}:{1}', self.address[0], self.address[1])

		# If self.client has a (active) socket, it is a client and
		# thus needs to starts syncing up with the server.
		if self.is_client:
			# Lock the client until the filelist has been sent back by the server.
			self.client._rlock.acquire()
			self.send('filelist', self.client._filelist)


	def close(self):
		self.sync_queue.put({'action': 'exit'})

		try:
			self.sock.shutdown(socket.SHUT_RDWR)
		except OSError:
			pass


	def handle(self, data):
		try:
			data = json.loads(data)
		except ValueError:
			log.error('Received invalid data: {0}', data)
			return

		route = data['route']
		data = data['data']

		log.info_v('Handling {0}, data:\n{1}', route, data)

		if route in self.routes:
			self.routes[route].handle(data, self)
		else:
			log.error("Data received on unknown route '{0}'!", route)


	def send(self, route, data):
		request = json.dumps({'route': route, 'data': data}) + '\n'

		self.sock.sendall(request.encode())


	def recv(self):
		if self.recvbuf:
			# This needs special handling because there could be multiple
			# request in recvbuf. If this is the case, we can only yield the first
			# one and have to leave to others in recvbuf.

			index = self.recvbuf.find(b'\n')

			if index == -1:
				yield self.recvbuf
				self.recvbuf = None
			else:
				yield self.recvbuf[:index]
				self.recvbuf = self.recvbuf[index+1:]
				return

		while 1:
			select.select([self.sock], [], [])

			chunk = self.sock.recv(1024)
			if not len(chunk):
				# If select has signaled the socket is readable, yet .recv()
				# returns zero bytes, the other end probably performed
				# a close() or shutdown() on the socket.
				break

			index = chunk.find(b'\n')

			if index == -1:
				yield chunk
			else:
				yield chunk[:index]
				self.recvbuf = chunk[index+1:]
				break


	def queue_file(self, action, path):
		self.sync_queue.put({
			'action': action + '-file',
			'path': path
		})


	def queue_event(self, event):
		self.sync_queue.put({
			'action': 'send-event',
			'event': event
		})


	def sync_worker(self):
		while 1:
			entry = self.sync_queue.get()
			log.info_v('Processing {0}', entry)

			if entry['action'] == 'exit':
				break

			elif entry['action'] == 'send-file':
				path = entry['path']
				abspath = self.client._abspath(path)

				self.send('file', {
					'path': path,
					'action': 'receive'
				})

				for buf in utils.read_file_seq(abspath, BLOCK_SIZE):
					self.sock.sendall(base64.b64encode(buf))
				self.sock.sendall(b'\n')

				self.client._event_handler._dispatch(
					'sent', self.client, path, 'file'
				)

			elif entry['action'] == 'request-file':
				self.send('file', {
					'path': entry['path'],
					'action': 'send'
				})

			elif entry['action'] == 'send-event':
				self.send('event', entry['event'])

			self.sync_queue.task_done()


	def run(self):
		self.setup()

		utils.create_thread(self.sync_worker,
			name=self.name.replace('request-handler', 'sync-worker'))

		while 1:
			buffer = b''
			for chunk in self.recv():
				buffer += chunk

			if not len(buffer):
				break

			self.handle(buffer.decode())


		log.info('Disconnected from {0}:{1}', self.address[0], self.address[1])
		self.close()


