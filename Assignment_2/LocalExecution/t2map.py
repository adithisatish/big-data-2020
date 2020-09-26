#!/usr/bin/python3

# Input:	adj_list, v(file handling)
# Output:	destination		contribution	page_rank

import sys

#Mapper reads input from "adj_list" and "v" and for each node i, it emits key, value pairs.

with open('/home/manah/BD/A2/v') as f:
	initial_page_ranks = f.read()
ipr = initial_page_ranks.split("\n")

nodes = {} #dictionary to keep track of all (destination) nodes

v = {}
for i in ipr[:-1]:
	v[i.split(",")[0]] = i.split(",")[1]

for line in sys.stdin:
	try:
		line = line.strip()
		source, destination = line.split("\t", 1)
		destination = destination.replace('[','').replace(']','').split(',')
		contribution = 1/len(destination)
	
	except:
		continue
	for d in destination:
		print(d, contribution, v[source], sep = '\t')
		#nodes[source] = True #Implies that this source has outgoing edges
		nodes[d] = True #Implies that this node has incoming edges and hence is a destination and will have its page rank affected in the reducer

for node in v:
	if node not in nodes: #Only outgoing edges, no incoming edges => Source => no parent nodes => no contribution by parent 
		printf(node, 0, "Source",sep = "\t") #This is so that the reducer knows that it's a source node