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
import socket
import sqlite3
import threading
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from behem0th import utils
from behem0th.RequestHandler import RequestHandler


IGNORE_LIST = [
	'.git',
	'.DS_Store'
]

DEFAULT_PORT = 3078


class _FsEventHandler(PatternMatchingEventHandler):
	def __init__(self, client):
		super().__init__(ignore_patterns=client._ignore_list)
		self._client = client


	def on_created(self, event):
		self._client._add_event(event)


	def on_deleted(self, event):
		self._client._add_event(event)


	def on_modified(self, event):
		self._client._add_event(event)


	def on_moved(self, event):
		self._client._add_event(event)


class Client:
	"""The main interface for behem0th

	This class can either act as a client which connects to a behem0th server
	or as a server to which behem0th clients can connect.

	Parameters
	----------
	path : :obj:`str`, optional
		The path which should be sync'd.
	folder : :obj:`folder`, optional
		The name of the folder which will contain behem0th-only
		data needed for syncing.

		behem0th will _not_ sync this folder!

	"""

	def __init__(self, path='.', folder='.behem0th'):
		self._sock = None
		self._rlock = threading.RLock()
		self._peers = []

		self._sync_path = os.path.abspath(path)
		self._meta_folder = folder

		path = os.path.join(self._sync_path, self._meta_folder)
		if not os.path.exists(path):
			os.mkdir(path, 0o755)

		self._ignore_list = IGNORE_LIST + [self._meta_folder]

		self._filetree = {
			'type': 'dir',
			'path': '',
			'files': {}
		}

		self._observer = Observer()
		self._observer.schedule(_FsEventHandler(self), self._sync_path, recursive=True)


	def connect(self, host, port=DEFAULT_PORT):
		"""Connects to a behem0th server

		Parameters
		----------
		host : :obj:`str`
			Hostname/IP of the behem0th server

		port : :obj:`int`, optional
			Port of the behem0th server

		"""

		self._collect_files()

		address = (host, port)
		self._sock = socket.socket()
		self._sock.connect(address)
		self._server = RequestHandler(sock=self._sock, address=address, client=self)
		self._server.start()
		self._observer.start()


	def listen(self, port=DEFAULT_PORT):
		"""Starts a behem0th server instance

		Parameters
		----------
		port : :obj:`int`, optional
			Port on which the server should be started

		"""

		self._collect_files()

		address = ('0.0.0.0', port)
		utils.create_thread(self._accept_worker, name='accept-worker', args=(address,))
		self._observer.start()


	def close(self):
		"""Closes all request handlers, writes the sync cache and shuts
		down the client.

		"""

		self._run_on_peers('close')

		self._observer.stop()
		self._write_db()


	def get_files(self):
		"""
		Returns
		-------
		:obj:`list`
			A list of all currently sync'd files.

		"""

		with self._rlock:
			ret = self._get_files(self._filetree)

		return ret


	def open_file(self, path, mode='r'):
		"""Opens a currently sync'd file for reading/writing.

		Parameters
		----------
		path : :obj:`str`
			Relative path of the file

		mode : :obj:`str`, optional
			Opening mode for the file (default: reading)
			Format is the same as :func:`open`

		"""

		return None


	def get_peers():
		"""
		Returns
		-------
		:obj:`list`
			A list of IP-address of all currently connected devices

		"""
		return [p.address for p in self._peers]


	def _lock(self):
		self._rlock.acquire()


	def _unlock(self):
		self._rlock.release()


	def _get_files(self, tree, relpath=''):
		ret = []

		for name, file in tree['files'].items():
			if file['type'] == 'dir':
				ret += self.get_files(file, os.path.join(relpath, name))
			else:
				ret.append(os.path.join(relpath, name))

		return ret


	def _collect_files(self):
		self._lock()

		for root, dirs, files in os.walk(self._sync_path):
			files[:] = [f for f in files if f not in self._ignore_list]
			dirs[:] = [d for d in dirs if d not in self._ignore_list]

			for name in files:
				self._add_to_filetree(os.path.join(root, name), 'file')

			for name in dirs:
				self._add_to_filetree(os.path.join(root, name), 'dir')

		self._unlock()


	# Must be called holding _lock
	# path must be an absolute path
	def _get_filetree_entry(self, path):
		path = os.path.relpath(path, self._sync_path)
		entry = self._filetree
		splitted = path.split('/')

		for part in splitted[:-1]:
			if not part in entry['files'] or entry['files'][part]['type'] != 'dir':
				return

			entry = entry['files'][part]

		return (entry, path, splitted[-1])


	# Must be called holding _lock
	# path must be an absolute path
	def _add_to_filetree(self, path, type):
		entry, relpath, name = self._get_filetree_entry(path)

		entry['files'][name] = {
			'type': type,
			'path': relpath,
			'files': {}
		}


	# Must be called holding _lock
	# path must be an absolute path
	def _remove_from_filetree(self, path):
		entry, relpath, name = self._get_filetree_entry(path)

		del entry['files'][name]


	def _merge_filetree(self, filetree):
		self._lock()

		for name, info in filetree['files'].items():
			if name not in self._filetree['files']:
				self._add_to_filetree(name, info['type'])

		self._unlock()


	def _replace_filetree(self, filetree):
		with self._rlock:
			self._filetree = filetree


	def _add_event(self, evt):
		self._lock()

		type = 'dir' if evt.is_directory else 'file'

		if evt.event_type == 'created':
			self._add_to_filetree(evt.src_path, type)

		elif evt.event_type == 'deleted':
			self._remove_from_filetree(evt.src_path)

		self._run_on_peers('queue_event', {
			'type': type + '-' + evt.event_type,
			'path': evt.src_path
		})

		self._unlock()


	def _run_on_peers(self, method, *args, **kwargs):
		for peer in self._peers:
			getattr(peer, method)(*args, **kwargs)


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
				path text not null
			)
		''')


		for file in self.get_files():
			try:
				conn.execute(
					'insert into files(hash, path) values (?, ?)',
					(utils.hash_file(file), file)
				)
			except FileNotFoundError:
				utils.log(
					"File '{0}' not found - possibly not fully sync'd!\n"
					"(This can normally be safely ignored - as soon as you reconnect, this device will sync up.)",
				file)

		conn.commit()
		conn.close()


	def _accept_worker(self, address):
		accept_sock = socket.socket()
		accept_sock.bind(address)
		accept_sock.listen()

		while 1:
			sock, address = accept_sock.accept()
			handler = RequestHandler(sock=sock, address=address, client=self)
			handler.start()
