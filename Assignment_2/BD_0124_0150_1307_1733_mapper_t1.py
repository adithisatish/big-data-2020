#!/usr/bin/python3

# Input:  source_node	dest_node
# Output: source_node	dest_node	--> sorted
import sys

for line in sys.stdin:
	if(line[0] == '#'):					# To ignore the first few commented junk lines, which were present in the Stanford sample data
		continue
	try:
		line = line.strip()
		source, dest = line.split("\t", 1)
		print(source, dest, sep = "\t")
	except:
		continue
