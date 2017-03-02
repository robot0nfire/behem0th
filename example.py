#!/usr/bin/env python3

import sys
import os

# Add the parent directory of the repo to the module search path
# to be able to import behem0th
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/..'))

import behem0th


class EventLoggingHandler(behem0th.EventHandler):
	def __init__(self):
		super().__init__()

	def on_created(self, event):
		print('on_created:', event)

	def on_modified(self, event):
		print('on_modified:', event)

	def on_deleted(self, event):
		print('on_deleted:', event)

	def on_moved(self, event):
		print('on_moved:', event)

	def on_sent(self, event):
		print('on_sent:', event)

	def on_received(self, event):
		print('on_received:', event)


c = behem0th.Client(event_handler=EventLoggingHandler(), verbose_log=True)

if len(sys.argv) == 2 and sys.argv[1] == 'listen':
	c.listen()
	input()

else:
	try:
		c.connect('127.0.0.1')
	except ConnectionRefusedError:
		print('Unable to connect!')
		sys.exit(1)

	input()

c.close()
