#!/usr/bin/python3

# Input:  source	dest
# Output: 
#	  source	[dest1, dest2,..]	--> adj_list
#	  source,initial_page_rank		--> v - locally stored

import sys
import os
from subprocess import Popen, PIPE

curr_key = None
keys = set()
values = []

for line in sys.stdin:
	line = line.strip()
	try:
		key, value = line.split("\t", 1)
	except:
		print("Line: ", line)
		
	if curr_key == key:
		values.append(value)
		keys.add(value)

	else:
		keys.add(key)
		
		if curr_key:
			print(curr_key, values, sep = "\t")
		curr_key = key
		values = [value]

print(curr_key, values, sep = "\t")						# For the last node.

keys.add(curr_key)
for i in values:
	keys.add(i)

										# Write the initial page ranks into a local file.
f = open("/home/manah/BD/A2/v", "w")
for i in keys:
	f.write(str(i) + "," + str(1) + "\n")
f.close()
