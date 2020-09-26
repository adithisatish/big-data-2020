#!/usr/bin/python3

# Input: destination	contribution	page_rank
# Output: v1: node,new_page_rank

import sys

curr_dest = None
chk = 0 #Flag to check for source nodes 
sum_contributions = 0
new_pageranks = {}

for line in sys.stdin:
	line = line.strip()

	dest, cont, v = line.split('\t')

	if v== "Source":
		new_page_rank[dest] = 1
		chk = 1 #Flag variable to check if node has no incoming edges
	
	if dest == curr_dest:
		if cont==0:
			sum_contributions = 1 #is it this or sum_cont+=1?
		else:
			sum_contributions += (float(cont) * float(v))
	else:
		if curr_dest:
			page_rank = 0.15 + 0.85*(sum_contributions)
			new_pageranks[curr_dest] = page_rank
		curr_dest = dest
		sum_contributions = float(cont)*float(v)

if chk !=1:		
	page_rank = 0.15 + 0.85*(sum_contributions)
	new_pageranks[curr_dest] = page_rank

# Write new page ranks to a file v1 --> Locally or Hdfs?	
for k, v in new_pageranks.items():
	k = k.strip('\'')
	print(k,v, sep=",")
