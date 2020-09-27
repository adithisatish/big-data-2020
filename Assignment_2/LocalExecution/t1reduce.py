#!/usr/bin/python3

# Input:  source	dest
# Output: 
#	  source	[dest1, dest2,..]	--> adj_list
#	  source,initial_page_rank		--> v - locally stored

import sys
import os
from subprocess import Popen, PIPE

curr_key = None
keys = set()									# Holds ALL nodes. Usage of set to handle addition into set better.
values = []

for line in sys.stdin:
	line = line.strip()
	try:
		key, value = line.split("\t", 1)
	except:
		print("Line: ", line)
		
	if curr_key == key:
		values.append(value)
		keys.add(value)						# The key has already been added. Add value to keys.
	else:
		keys.add(key)
		keys.add(value)					
		if curr_key:
			print(curr_key, values, sep = "\t")
		curr_key = key
		values = [value]

print(curr_key, values, sep = "\t")						# For the last node.

# I don't think the following adds are necessary.
keys.add(curr_key)
for i in values:
	keys.add(i)


fileName = sys.argv[1]										# Write the initial page ranks into a local fi
f = open(fileName, "w")
for i in sorted(keys):
	f.write(str(i) + "," + str(1) + "\n")
f.close()
