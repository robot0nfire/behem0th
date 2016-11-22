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
from functools import partial
from behem0th import utils


class RequestHandler(threading.Thread):
	req_handler_num = 0

	def __init__(self, **kwargs):
		super().__init__()
		self.daemon = True
		self._sync_list = []
		self._sync_list_cv = threading.Condition()

		RequestHandler.req_handler_num += 1
		self.name = "request-handler-{0}".format(RequestHandler.req_handler_num)
		for key, value in kwargs.items():
			setattr(self, key, value)

		with self.client._rlock:
			self.client._peers.append(self)

		self._is_client = bool(self.client._sock)


	def setup(self):
		self.log('Connected to {0}', self.address)

		# If self.client has a (active) socket, it is a client and
		# thus needs to starts syncing up with the server.
		if self._is_client:
			# Lock the client until the filelist has been sent back by the server.
			self.client._rlock.acquire()
			self.send('filelist', self.client._filetree)


	def close(self):
		try:
			self.sock.shutdown(socket.SHUT_RDWR)
		except OSError:
			pass


	def handle(self, what, data):
		info = json.loads(data.decode('utf-8'))

		if what == 'filelist':
			if self._is_client:
				self.client._filetree = info
				self.client._rlock.release()
			else:
				self.client._merge_filetree(info)
				self.send('filelist', self.client._filetree)

		elif what == 'file':
			with open(info['path'], 'wb') as f:
				for buf in iter(partial(self.sock.recv, 4096), b''):
					f.write(buf)

		elif what == 'event':
			# TODO
			pass


	def send(self, what, data):
		what = bytes(what + '\n', 'utf-8')

		if type(data) == dict:
			data = json.dumps(data)

		if type(data) == str:
			data = what + bytes(data, 'utf-8')
		else:
			data = what + bytes(data)

		self.sock.sendall(struct.pack('<I', len(data)) + data)


	def queue_file(self, path):
		with self._sync_list_cv:
			self._sync_list.append({
				'action': 'send-file'
				'path': path
			})

			self._sync_list_cv.notify()


	def queue_event(self, event):
		with self._sync_list_cv:
			self._sync_list.append({
				'action': 'send-event',
				'event': event
			})

			self._sync_list_cv.notify()


	def sync_worker(self):
		while 1:
			entry = None

			with self._sync_list_cv:
				self._sync_list_cv.wait()
				entry = self._sync_list.pop(0)

			if entry['action'] == 'send-file':
				path = entry['path']
				info = json.dumps({
					'path': path,
					'size': os.path.getsize(path)
				})

				self.send('file', info)
				for buf in utils.read_file_seq(path):
					self.sock.sendall(buf)

			elif entry['action'] == 'send-event':
				info = json.dumps(entry['event'])

				self.send('event', info)


	def run(self):
		self.setup()

		try:
			utils.create_thread(self.sync_worker, name='{0}_sync-worker'.format(self.name))

			while 1:
				info = self.sock.recv(4)
				if not len(info):
					break

				info = struct.unpack('<I', info)

				data = self.sock.recv(info[0])
				data = data.split(b'\n', 1)
				if len(data) != 2:
					self.log('\n\nReceived invalid data:\n{0}\n\n', data)
				else:
					self.handle(data[0].decode('utf-8'), data[1])

		except ConnectionResetError:
			self.log('Lost connection to {0}', self.address)
		else:
			self.log('Disconnected from {0}', self.address)

		self.close()


	def log(self, str, *args, **kwargs):
		utils.log('{__name}: ' + str, *args, __name=self.name, **kwargs)
