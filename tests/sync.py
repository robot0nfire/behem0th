#!/usr/bin/env python3


import sys
import errno
import time
import behem0th


c = behem0th.Client()

if len(sys.argv) == 2 and sys.argv[1] == 'listen':
	c.listen()

	print('started listening ...\n')
	input()

else:
	try:
		c.connect('127.0.0.1')
	except ConnectionRefusedError:
		print('Unable to connect!')
		sys.exit(1)

	input()

c.close()
