#!/usr/bin/python3

# Input:	adj_list, v(file handling)
# Output:	destination		contribution	page_rank

import sys

#Mapper reads input from "adj_list" and "v" and for each node i, it emits key, value pairs.

with open('/home/manah/BD/A2/v') as f:
	initial_page_ranks = f.read()
ipr = initial_page_ranks.split("\n")

v = {}
for i in ipr[:-1]:
	v[i.split("\t")[0]] = i.split("\t")[1]

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

