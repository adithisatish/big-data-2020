#!/usr/bin/python3

# Input:	adj_list, v(file handling)
# Output:	destination		contribution	page_rank

import sys

#Mapper reads input from "adj_list" and "v" and for each node i, it emits key, value pairs.

fileName = sys.argv[1];
with open(fileName) as f:									# Read v
	initial_page_ranks = f.read()
ipr = initial_page_ranks.split("\n")

nodes = {}	# Holds all the destination nodes. Dictionary											
#nodes = set()
v = {}
for i in ipr[:-1]:
	v[i.split(",")[0]] = i.split(",")[1]

for line in sys.stdin:
	try:
		line = line.strip()
		source, destination = line.split("\t", 1)
		destination = destination.replace('[','').replace(']','').split(', ')
		contribution = 1/len(destination)
	
	except:
		continue

	for d in destination:
		
		d = d.strip('\'')
		print(d, contribution, v[source], sep = '\t', end='\n')
		#nodes.add(d.strip('\''))
		nodes[d] = True
	
for node in v:
	if node not in nodes:
		print(node, 0, 1, sep = "\t")				# Contribution is 0. Initial Page Rank is 1. (Becomes0.15 in the next step)

