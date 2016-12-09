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

import threading
import hashlib
from string import Formatter
from functools import partial


def log(str, *args, **kwargs):
	str = Formatter().vformat(str, args, kwargs)
	str = str.replace('\n', '\n' + ' ' * 11)

	print('[behem0th]', str)


def create_thread(target, args=(), name=None):
	thread = threading.Thread(target=target, args=args)
	if name:
		thread.name = name
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
