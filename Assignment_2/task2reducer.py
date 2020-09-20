#!/usr/bin/python3

import sys

curr_dest = None

contibutions = []

with open('/path/to/v') as f:
	initial_page_ranks = f.read()
v1 = initial_page_ranks.split("\n")

for line in sys.stdin:
	line = line.strip()
	dest, source, cont = line.split("\t", 1)
	
	if dest == curr_dest:
		contributions.append(cont* v1[source])
	else:
		if curr_dest:
			page_rank = 0.15 + 0.85(sum(contributions))
			v1[dest] = page_rank
		curr_dest = dest
		contributions = [cont*v1[source]]
		
