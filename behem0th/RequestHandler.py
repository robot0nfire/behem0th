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
from behem0th import utils, log


class RequestHandler(threading.Thread):
	req_handler_num = 0

	def __init__(self, **kwargs):
		super().__init__()
		self.daemon = True
		self.sync_queue = queue.Queue()

		RequestHandler.req_handler_num += 1
		self.name = "request-handler-{0}".format(RequestHandler.req_handler_num)
		for key, value in kwargs.items():
			setattr(self, key, value)

		with self.client._rlock:
			self.client._peers.append(self)

		self.is_client = bool(self.client._sock)


	def setup(self):
		log.info('Connected to {0}:{1}', self.address[0], self.address[1])

		# If self.client has a (active) socket, it is a client and
		# thus needs to starts syncing up with the server.
		if self.is_client:
			# Lock the client until the filelist has been sent back by the server.
			self.client._rlock.acquire()
			self.send('filelist', self.client._filelist)


	def close(self):
		try:
			self.sock.shutdown(socket.SHUT_RDWR)
		except OSError:
			pass


	def handle(self, what, data):
		info = json.loads(data.decode('utf-8'))
		log.info_v('Handling {0}, data:\n{1}', what, info)

		if what == 'filelist':
			if self.is_client:
				self.client._filelist = info
				self.client._rlock.release()
			else:
				files, events = self.client._merge_filelist(info)
				with self.client._rlock:
					self.send('filelist', self.client._filelist)

				for e in events:
					self.queue_event(e)
				for f in files:
					self.queue_file(f[0], f[1])


		elif what == 'file':
			self.client._ignore_next_fsevent(info['path'])
			if info['action'] == 'receive':
				size = info['size']

				with open(info['path'], 'wb') as f:
					buf = self.sock.recv(max(0, min(size, 4096)))
					while buf:
						f.write(buf)
						size -= 4096
						buf = self.sock.recv(max(0, min(size, 4096)))

			elif info['action'] == 'send':
				self.queue_file('send', info['path'])


		elif what == 'event':
			f_type, event = info['type'].split('-')
			path = info['path']

			self.client._ignore_next_fsevent(path)

			# TODO: factor out common code with Client._handle_fsevent() and Client._merge_filelist()
			# TODO: Lock modified files until they are completely transfered.
			if event == 'created':
				self.client._add_to_filelist(path, f_type)

				# create the file/directory
				if f_type == 'file':
					open(path, 'a').close()
				else:
					os.mkdir(path, 0o755)


			elif event == 'deleted':
				self.client._remove_from_filelist(path)
				os.remove(path)

			elif event == 'moved':
				self.client._remove_from_filelist(path)
				self.client._add_to_filelist(info['dest'], f_type)
				os.rename(path, info['dest'])


	def send(self, what, data):
		what = bytes(what + '\n', 'utf-8')

		if type(data) == dict:
			data = json.dumps(data)

		if type(data) == str:
			data = what + bytes(data, 'utf-8')
		else:
			data = what + bytes(data)

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

			if entry['action'] == 'send-file':
				path = entry['path']

				self.send('file', {
					'path': path,
					'size': os.path.getsize(path),
					'action': 'receive'
				})

				for buf in utils.read_file_seq(path):
					self.sock.sendall(buf)

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


