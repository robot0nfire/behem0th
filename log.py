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

from string import Formatter
import sys
import inspect
import threading

global NO_LOG
global VERBOSE_LOG

if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
	CLEAR_FORMAT = '\033[0m'
	WARN_COLOR = '\033[33m'
	ERR_COLOR = '\033[31m'
	BEHEM0TH_COLOR = '\033[94m'
	CLASS_COLOR = '\033[96m'
	THREAD_COLOR = '\033[92m'
else:
	CLEAR_FORMAT = WARN_COLOR = ERR_COLOR = BEHEM0TH_COLOR = CLASS_COLOR = THREAD_COLOR = ''


def _print(str, *args, **kwargs):
	if NO_LOG:
		return

	str = Formatter().vformat(str, args, kwargs)
	str = str.replace('\n', '\n' + ' ' * 11)

	stack = inspect.stack()

	# Stack depth has to be atleast 3 normally, since _print() should
	# be normally only be called from info{_v}(), warn() or error().
	# But we still take care of edge cases where this may not be true.
	frame = stack[2][0] if len(stack) >= 3 else None
	class_ = frame.f_locals['self'].__class__.__name__ if frame and 'self' in frame.f_locals else '<unknown>'
	thread = threading.current_thread().name

	print(BEHEM0TH_COLOR + '[behem0th]',
		CLASS_COLOR + '[' + class_ + ']',
		THREAD_COLOR + thread + ':', str, CLEAR_FORMAT)


def info(str, *args, **kwargs):
	_print(CLEAR_FORMAT + str, *args, **kwargs)


def warn(str, *args, **kwargs):
	_print(WARN_COLOR + str, *args, **kwargs)


def error(str, *args, **kwargs):
	_print(ERR_COLOR + str, *args, **kwargs)


def info_v(str, *args, **kwargs):
	if VERBOSE_LOG:
		_print(CLEAR_FORMAT + str, *args, **kwargs)
