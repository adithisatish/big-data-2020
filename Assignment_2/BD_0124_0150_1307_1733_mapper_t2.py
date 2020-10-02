#!/usr/bin/python3

# Input:	adj_list(Hadoop), v(file handling)
# Output:	destination		contribution

import sys

#Mapper reads input from "adj_list" and "v" and for each node i, it emits key, value pairs.

fileName = sys.argv[1]
with open(fileName) as f:				# Read v
	initial_page_ranks = f.read()
ipr = initial_page_ranks.split("\n")

nodes = {} 		# All destination nodes. Dict											
#nodes = set()

v = {}			# Holds page ranks. Dict. source : page_rank
for i in ipr[:-1]:
	v[i.split(", ")[0]] = i.split(", ")[1]


for line in sys.stdin:
	try:
		line = line.strip()
		source, destination = line.split("\t", 1)
		destination = destination.replace('[','').replace(']','').split(', ')
		contribution = float(v[source])/len(destination)	# p_r of node/ no_of_outgoing_links
	
	except:
		continue

	for d in destination:
		d = d.strip('\'')
		if d in v:						# Only compute ranks of nodes that were a source. This is the input to t2reduce.
			print(d, contribution, sep = '\t', end='\n')
			nodes[d] = True				# Add to dict of destination nodes. Can use a set/list instead.
	
for node in v:			# If there is a source node (all source nodes have a pagerank)
	if node not in nodes:	# that was not a destination, and hence wasn't printed to t2reduce,..
		print(node, 0, sep = '\t')	# The node has outgoing edges, but no incoming edges. Contribution is 0. Page rank should ultimatekly be 1. -> 0.15 + 0.85*0 = 0.15

