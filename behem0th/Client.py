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
from functools import wraps
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from behem0th import utils
from behem0th.RequestHandler import RequestHandler


IGNORE_LIST = [
	'.git',
	'.DS_Store'
]

DEFAULT_PORT = 3078


def synchronized(fn):
	@wraps(fn)
	def wrap(*args, **kwargs):
		lock = args[0]._rlock
		with lock:
			return fn(*args, **kwargs)

	return wrap


class _FsEventHandler(PatternMatchingEventHandler):
	def __init__(self, client):
		super().__init__(ignore_patterns=client._ignore_list)
		self._client = client


	def on_any_event(self, event):
		# Ignore DirModifiedEvent's completely.
		if event.event_type == 'modified' and event.is_directory:
			return

		self._client._handle_fsevent(event)


class _AcceptWorker(threading.Thread):
	def __init__(self, **kwargs):
		super().__init__()
		self.name = 'accept-worker'
		self.kwargs = kwargs
		self.daemon = True


	def run(self):
		client = self.kwargs['client']

		accept_sock = socket.socket()
		accept_sock.bind(self.kwargs['address'])
		accept_sock.listen()

		while 1:
			sock, address = accept_sock.accept()
			RequestHandler(sock=sock, address=address, client=client).start()


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

		self._filelist = {}
		self._observer = Observer()
		self._observer.schedule(_FsEventHandler(self), self._sync_path, recursive=True)

		self._fsevent_ignore_list = []


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

		_AcceptWorker(address=('0.0.0.0', port), client=self).start()
		self._observer.start()


	def close(self):
		"""Closes all request handlers, writes the sync cache and shuts
		down the client.

		"""

		self._run_on_peers('close')

		self._observer.stop()
		self._write_db()


	@synchronized
	def get_files(self):
		"""
		Returns
		-------
		:obj:`list`
			A list of all currently sync'd files.

		"""

		return [
			{'path': f['path'], 'type': f['type']}
				for f in self._filelist.values()
		]


	@synchronized
	def get_peers(self):
		"""
		Returns
		-------
		:obj:`list`
			A list of IP-address of all currently connected devices

		"""
		return [p.address for p in self._peers]


	@synchronized
	def _collect_files(self):
		for root, dirs, files in os.walk(self._sync_path):
			files[:] = [f for f in files if f not in self._ignore_list]
			dirs[:] = [d for d in dirs if d not in self._ignore_list]

			relpath = os.path.relpath(root, self._sync_path)

			for name in files:
				self._add_to_filelist(os.path.join(relpath, name), 'file')

			for name in dirs:
				self._add_to_filelist(os.path.join(relpath, name), 'dir')


	@synchronized
	def _merge_filelist(self, filelist):
		files = []
		events = []

		for file, info in filelist.items():
			if not file in self._filelist:
				self._add_to_filelist(file, info['type'])
				self._ignore_next_fsevent(file)

				if info['type'] == 'file':
					open(file, 'a').close()
					files.append(('request', file))
				else:
					os.mkdir(file, 0o755)

		for file, info in self._filelist.items():
			if not file in filelist:
				if info['type'] == 'file':
					events.append({'type': 'file-created', 'path': file})
					files.append(('send', file))
				else:
					events.append({'type': 'dir-created', 'path': file})

		return (files, events)


	@synchronized
	def _add_to_filelist(self, path, type):
		path = os.path.normpath(path)
		self._filelist[path] = {'path': path, 'type': type}


	@synchronized
	def _remove_from_filelist(self, path):
		del self._filelist[os.path.normpath(path)]


	@synchronized
	def _handle_fsevent(self, evt):
		path = os.path.relpath(evt.src_path, self._sync_path)
		type = 'dir' if evt.is_directory else 'file'

		if path in self._fsevent_ignore_list:
			self._fsevent_ignore_list.remove(path)
			return

		remote_event = {'type': type + '-' + evt.event_type, 'path': path}

		if evt.event_type == 'created':
			self._add_to_filelist(path, type)

		elif evt.event_type == 'deleted':
			self._remove_from_filelist(path)

		elif evt.event_type == 'moved':
			self._remove_from_filelist(path)

			path = os.path.relpath(evt.dest_path, self._sync_path)
			self._add_to_filelist(path, type)
			remote_event['dest'] = path

		if evt.event_type != 'modified':
			self._run_on_peers('queue_event', remote_event)
		elif type == 'file':
			self._run_on_peers('queue_file', 'send', path)


	@synchronized
	def _ignore_next_fsevent(self, path):
		self._fsevent_ignore_list.append(path)


	@synchronized
	def _run_on_peers(self, method, *args, **kwargs):
		for peer in self._peers:
			getattr(peer, method)(*args, **kwargs)


	@synchronized
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


		for file, info in self._filelist.items():
			if info['type'] == 'dir':
				continue

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
