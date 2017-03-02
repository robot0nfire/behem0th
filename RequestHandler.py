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
from behem0th import utils, log


class Route:
	def handle(self, data, request):
		raise NotImplementedError


	def send(self, data):
		self.handler.send(self.route_name, data)


	def recv(self, size):
		return self.handler.sock.recv(size)


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


class FileRoute(Route):
	def handle(self, data, request):
		action = data['action']
		path = data['path']

		request.client._ignore_next_fsevent(path)
		if action == 'receive':
			size = data['size']
			tmpf = tempfile.NamedTemporaryFile(delete=False)

			buf = self.recv(max(0, min(size, 4096)))
			while buf:
				tmpf.write(buf)
				size -= 4096
				buf = self.recv(max(0, min(size, 4096)))

			tmpf.close()
			os.rename(tmpf.name, request.client._abspath(path))

			request.client._update_metadata(path)
			request.client._event_handler._dispatch(
				'received', request.client, path, 'file'
			)

		elif action == 'send':
			request.queue_file('send', path)


class EventRoute(Route):
	def handle(self, data, request):
		f_type, event = data['type'].split('-')
		path = data['path']
		abspath = request.client._abspath(path)

		request.client._ignore_next_fsevent(path)

		# TODO: factor out common code with Client._handle_fsevent() and Client._merge_filelist()
		# TODO: Lock modified files until they are completely transfered.
		if event == 'created':
			request.client._add_to_filelist(path, f_type)

			# create the file/directory
			if f_type == 'file':
				open(abspath, 'a').close()
			else:
				os.mkdir(abspath, 0o755)

		elif event == 'deleted':
			request.client._remove_from_filelist(path)
			os.remove(abspath)

		elif event == 'moved':
			request.client._remove_from_filelist(path)
			request.client._add_to_filelist(data['dest'], f_type)
			os.rename(abspath, data['dest'])


ROUTES = {
	'filelist': FilelistRoute(),
	'file': FileRoute(),
	'event': EventRoute()
}


class RequestHandler(threading.Thread):
	req_handler_num = 0

	def __init__(self, **kwargs):
		super().__init__()
		self.daemon = True
		self.sync_queue = queue.Queue()
		self.routes = {}

		RequestHandler.req_handler_num += 1
		self.name = "request-handler-{0}".format(RequestHandler.req_handler_num)
		for key, value in kwargs.items():
			setattr(self, key, value)

		with self.client._rlock:
			self.client._peers.append(self)

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


	def handle(self, route, data):
		data = json.loads(data.decode('utf-8'))
		log.info_v('Handling {0}, data:\n{1}', route, data)

		if route in self.routes:
			self.routes[route].handle(data, self)
		else:
			log.error("Data received on unknown route '{0}'!", route)


	def send(self, route, data):
		route = bytes(route + '\n', 'utf-8')

		if type(data) == dict:
			data = json.dumps(data)

		if type(data) == str:
			data = route + bytes(data, 'utf-8')
		else:
			data = route + bytes(data)

		self.sock.sendall(struct.pack('<I', len(data)) + data)


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

			if entry['action'] == 'send-file':
				path = entry['path']
				abspath = self.client._abspath(path)

				self.send('file', {
					'path': path,
					'size': os.path.getsize(abspath),
					'action': 'receive'
				})

				for buf in utils.read_file_seq(abspath):
					self.sock.sendall(buf)

				self.client._event_handler._dispatch(
					'sent', self.client, path, 'file'
				)

			if entry['action'] == 'request-file':
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
			info = self.sock.recv(4)
			if not info:
				break

			info = struct.unpack('<I', info)

			data = self.sock.recv(info[0])
			data = data.split(b'\n', 1)
			if len(data) != 2:
				log.error('Received invalid data:\n{0}', data)
			else:
				self.handle(data[0].decode('utf-8'), data[1])

		log.info('Disconnected from {0}:{1}', self.address[0], self.address[1])
		self.close()


