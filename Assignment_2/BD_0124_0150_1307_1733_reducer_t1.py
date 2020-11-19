#!/usr/bin/python3

# Input:  source	dest
# Output: 
#	  source	[dest1, dest2,..]	--> adj_list
#	  source,initial_page_rank		--> v - locally stored

import sys
import os

curr_key = None
keys = set()						# Set to hold all the source nodes.
values = []						# Holds destination set for a particular source node. [d1, d2, d3..]

for line in sys.stdin:
	line = line.strip()
	try:
		key, value = line.split("\t", 1)
	except:
		continue
		
	if curr_key == key:				# Reading a source that's been encountered
		values.append(value)

	else:						# Reading a new source
		keys.add(key)
		if curr_key:
			print(curr_key, values, sep = "\t")
		curr_key = key
		values = [value]

print(curr_key, values, sep = "\t")				# For the last node.


keys.add(curr_key)	


fileName = sys.argv[1]						# Write the initial page ranks into a local file
f = open(fileName, "w")
for i in sorted(keys):
	f.write(str(i) + ", " + str(1) + "\n")
f.close()
