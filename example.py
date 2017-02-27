#!/usr/bin/env python3

import sys
import os

# Add the parent directory of the repo to the module search path
# to be able to import behem0th
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/..'))

import behem0th

c = behem0th.Client(verbose_log=True)

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
