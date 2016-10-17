#
# Copyright (c) 2016 robot0nfire
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

import socket
import sqlite3
import threading
import sys, os, errno
import socketserver
import json
import struct
import time
import hashlib
from string import Formatter
from functools import partial


IGNORE_LIST = [
	'.git',
	'.DS_Store'
]

DEFAULT_PORT = 3078


def log(str, *args, **kwargs):
	print('[behem0th]', Formatter().vformat(str, args, kwargs))


def create_thread(target, **kwargs):
	args = kwargs['args'] if 'args' in kwargs else ()

	thread = threading.Thread(target=target, args=args)
	if 'name' in kwargs:
		thread.name = kwargs['name']
	thread.daemon = True
	thread.start()

	return thread


def read_file_seq(path):
	with open(path, 'rb') as f:
		for buf in iter(partial(f.read, 4096), b''):
			yield buf


def hash_file(path):
	h = hashlib.md5()
	h.update(bytes(path, 'utf-8'))
	for buf in read_file_seq(path):
		h.update(buf)

	return h.hexdigest()


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

		self._is_client = not not self.client._sock


	def setup(self):
		self.log('Connected to {0}', self.address)

		# If self.client has a (active) socket, it is a client and
		# thus needs to starts syncing up with the server.
		if self._is_client:
			# Lock the client until the filelist has been sent back by the server.
			self.client.lock()
			self.send('filelist', self.client._get_slim_filetree())


	def close(self):
		pass


	def handle(self, what, data):
		if what == 'filelist':
			tree = json.loads(data.decode('utf-8'))

			if self._is_client:
				# The client is still locked
				self.client._replace_filetree(tree)
				self.client.unlock()
			else:
				self.client._merge_filetree(tree)
				self.send('filelist', self.client._get_slim_filetree())

		elif what == 'file':
			info = json.loads(data.decode('utf-8'))
			with open(info['path'], 'wb') as f:
				for buf in iter(partial(self.sock.recv, 4096), b''):
					f.write(buf)


	def send(self, what, data):
		if type(data) == dict:
			data = json.dumps(data)

		if type(data) == str:
			data = bytes(what + '\n', 'utf-8') + bytes(data, 'utf-8')
		else:
			data = bytes(what + '\n', 'utf-8') + bytes(data)

		self.sock.sendall(struct.pack('<I', len(data)) + data)


	def queue_file(self, path, dir):
		with self._sync_list_cv:
			self._sync_list.append({ 'path': path, 'dir': dir })
			self._sync_list_cv.notify()


	def sync_worker(self):
		while 1:
			file = None

			with self._sync_list_cv:
				self._sync_list_cv.wait_for(lambda: len(self._sync_list))
				file = self._sync_list.pop(0)

			if file['dir'] == 'to':
				path = file['path']

				info = json.dumps({
					'path': path,
					'size': os.path.getsize(path)
				})

				self.send('file', info)

				for buf in read_file_seq(path):
					self.sock.sendall(buf)


	def run(self):
		self.setup()

		try:
			create_thread(self.sync_worker, name='{0}-sync_worker'.format(self.name))

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
		log('{__name}: ' + str, *args, __name=self.name, **kwargs)


class Client:
	def __init__(self, **kwargs):
		self._sock = None
		self._lock = threading.RLock()
		self._peers = []

		self._sync_path = os.path.abspath(kwargs['path'] if 'path' in kwargs else '.')
		self._meta_folder = kwargs['folder'] if 'folder' in kwargs else '.behem0th'

		path = os.path.join(self._sync_path, self._meta_folder)
		if not os.path.exists(path):
			os.mkdir(path, 0o755)

		self._ignore_list = IGNORE_LIST + [self._meta_folder]

		self._filetree = {
			'type': 'dir',
			'abspath': self._sync_path,
			'files': {}
		}


	def connect(self, host, port=DEFAULT_PORT):
		self._collect_files()

		address = (host, port)
		self._sock = socket.socket()
		self._sock.connect(address)
		self._peers.append(address)
		self._server = RequestHandler(sock=self._sock, address=address, client=self)
		self._server.start()


	def listen(self, port=DEFAULT_PORT):
		self._collect_files()

		address = ('0.0.0.0', port)
		create_thread(self._accept_worker, name='accept-worker', args=(address,))


	def close(self):
		if self._sock:
			self._sock.shutdown(socket.SHUT_RDWR)

		self._write_db()


	def get_files(self, tree=None, relpath=''):
		ret = []

		self.lock()
		if not tree:
			tree = self._filetree

		for name, file in tree['files'].items():
			if file['type'] == 'dir':
				ret += self.get_files(file, os.path.join(relpath, name))
			else:
				ret.append(os.path.join(relpath, name))

		self.unlock()
		return ret


	def open_file(self, path, dir=None):
		return None


	def get_peers():
		return self._peers


	def lock(self):
		self._lock.acquire()


	def unlock(self):
		self._lock.release()


	def _collect_files(self):
		self.lock()

		for root, dirs, files in os.walk(self._sync_path):
			files[:] = [f for f in files if f not in self._ignore_list]
			dirs[:] = [d for d in dirs if d not in self._ignore_list]

			for name in files:
				self._add_to_filetree(os.path.join(root, name), 'file')

			for name in dirs:
				self._add_to_filetree(os.path.join(root, name), 'dir')

		self.unlock()


	# Must be called holding _lock
	def _add_to_filetree(self, path, type):
		path = os.path.relpath(path, self._sync_path)
		entry = self._filetree
		splitted = path.split('/')

		for part in splitted[:-1]:
			if not part in entry['files'] or entry['files'][part]['type'] != 'dir':
				return

			entry = entry['files'][part]

		entry['files'][splitted[-1]] = {
			'type': type,
			'abspath': os.path.join(self._sync_path, path),
			'files': {}
		}


	def _get_slim_filetree(self, tree=None):
		ret = {}

		self.lock()
		if not tree:
			tree = self._filetree

		for name, file in tree['files'].items():
			ret[name] = {
				'type': file['type'],
				'files': {}
			}

			if file['type'] == 'dir':
				ret.update(self._get_slim_filetree(file))

		self.unlock()
		return ret


	def _merge_filetree(self, filetree):
		self.lock()

		for name, info in filetree.items():
			if name not in self._filetree['files']:
				self._add_to_filetree(name, info['type'])

		self.unlock()


	def _replace_filetree(self, filetree):
		self.lock()
		self._filetree['files'] = filetree
		self.unlock()


	def _write_db(self):
		file = os.path.join(self._meta_folder, 'file_index.db')
		try:
			os.remove(file)
		except OSError:
			pass

		conn = sqlite3.connect(file)

		conn.execute('''
			create table files (
				hash text primary key,
				path text not null,
				type text not null
			)
		''')


		for file in self.get_files():
			conn.execute(
				'insert into files(hash, path, type) values (?, ?, ?)',
				(hash_file(file), file, 'file')
			)

		conn.commit()
		conn.close()


	def _accept_worker(self, address):
		accept_sock = socket.socket()
		accept_sock.bind(address)
		accept_sock.listen()

		while 1:
			sock, address = accept_sock.accept()
			self._peers.append(address)
			handler = RequestHandler(sock=sock, address=address, client=self)
			handler.start()
