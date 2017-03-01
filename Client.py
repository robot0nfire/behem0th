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
import sys
import socket
import threading
from functools import wraps

BEHEM0TH_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.join(BEHEM0TH_PATH, 'watchdog/src'))
sys.path.append(os.path.join(BEHEM0TH_PATH, 'pathtools'))

from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, FileModifiedEvent

from behem0th import utils, log
from behem0th.RequestHandler import RequestHandler


IGNORE_LIST = [
	'.git/',
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
		self.client = client


	def on_any_event(self, event):
		# Ignore DirModifiedEvent's completely.
		if event.event_type == 'modified' and event.is_directory:
			return

		event_handled = self.client._handle_fsevent(event)
		log.info_v('Got event {0} - was handled? {1}', event, event_handled)

		# On macOS, watchdog only produces a file-created event,
		# on Linux however also a file-modified event is generated.
		if sys.platform == 'darwin':
			if event_handled and event.event_type == 'created' and not event.is_directory:
				self.client._handle_fsevent(FileModifiedEvent(event.src_path))


class _AcceptWorker(threading.Thread):
	def __init__(self, **kwargs):
		super().__init__()
		self.name = 'accept-worker'
		self.kwargs = kwargs
		self.daemon = True


	def run(self):
		client = self.kwargs['client']
		address = self.kwargs['address']

		accept_sock = socket.socket()
		accept_sock.bind(address)
		accept_sock.listen()

		log.info_v('Started listening on {0}:{1}', address[0], address[1])

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

	no_log : :obj:`bool`, optional
		If set to true, behem0th will not output any logging messages. (default)
		Otherwise, it will print some basic informations, e.g. when
		a client (dis)connects.
		Implies verbose_log = false

	verbose_log : :obj:`bool`, optional
		If set to true, behem0th will print some advanded (debugging) messages.
		Implies no_log = false

	"""

	def __init__(self, path='.', no_log=True, verbose_log=False):
		log.NO_LOG = no_log and not verbose_log
		log.VERBOSE_LOG = not log.NO_LOG and verbose_log

		self._sock = None
		self._rlock = threading.RLock()
		self._peers = []

		self._sync_path = os.path.abspath(path)

		self._ignore_list = IGNORE_LIST
		log.info_v('Ignored files/directories: {0}', self._ignore_list)

		self._filelist = {}
		self._observer = Observer()
		self._observer.schedule(_FsEventHandler(self), self._sync_path, recursive=True)
		log.info_v("Started watching folder '{0}'", self._sync_path)

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


	@synchronized
	def get_files(self):
		"""
		Returns
		-------
		:obj:`list`
			A list of all currently sync'd files.

		"""

		return [
			{'path': path, 'type': info['type']}
				for path, info in self._filelist.items()
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
		ignore_list = [os.path.normpath(e) for e in self._ignore_list]

		for root, dirs, files in os.walk(self._sync_path):
			files[:] = [f for f in files if f not in ignore_list]
			dirs[:] = [d for d in dirs if d not in ignore_list]

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
				abspath = self._abspath(file)

				if info['type'] == 'file':
					open(abspath, 'a').close()
					files.append(('request', file))
				else:
					os.mkdir(abspath, 0o755)

			elif info['type'] == 'file' and info['hash'] != self._filelist[file]['hash']:
				if info['mtime'] < self._filelist[file]['mtime']:
					files.append(('send', file))
				else:
					files.append(('request', file))

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
		abspath = self._abspath(path)

		self._filelist[os.path.normpath(path)] = {
			'type': type,
			'hash': utils.hash_file(abspath) if type == 'file' else '',
			'mtime': os.path.getmtime(abspath)
		}


	@synchronized
	def _remove_from_filelist(self, path):
		del self._filelist[os.path.normpath(path)]


	@synchronized
	def _update_metadata(self, path):
		path = os.path.normpath(path)
		abspath = self._abspath(path)

		self._filelist[path]['hash'] = utils.hash_file(abspath)
		self._filelist[path]['mtime'] = os.path.getmtime(abspath)


	@synchronized
	def _handle_fsevent(self, evt):
		path = os.path.relpath(evt.src_path, self._sync_path)
		type = 'dir' if evt.is_directory else 'file'

		if path in self._fsevent_ignore_list:
			self._fsevent_ignore_list.remove(path)
			return False

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
			self._update_metadata(path)
			self._run_on_peers('queue_file', 'send', path)

		return True


	@synchronized
	def _ignore_next_fsevent(self, path):
		self._fsevent_ignore_list.append(path)


	@synchronized
	def _run_on_peers(self, method, *args, **kwargs):
		for peer in self._peers:
			getattr(peer, method)(*args, **kwargs)

	def _abspath(self, path):
		return os.path.join(self._sync_path, path)
