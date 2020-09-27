#!/usr/bin/python3

# Input: destination	contribution	page_rank
# Output: v1: node,new_page_rank

import sys

curr_dest = None
sum_contributions = 0
new_pageranks = {}

for line in sys.stdin:
	line = line.strip()
	dest, cont, v = line.split('\t')
	dest = dest.strip('\'')
	if dest == curr_dest:
		sum_contributions += (float(cont)*float(v))
	else:
		if curr_dest:
			page_rank = 0.15 + 0.85*sum_contributions	# No incoming edges -> sum is 0 -> page_rank = 0.15
			new_pageranks[curr_dest] = page_rank
		curr_dest = dest
		sum_contributions = float(cont)*float(v)
		
page_rank = 0.15 + 0.85*(sum_contributions)
new_pageranks[curr_dest] = page_rank


for k in sorted(new_pageranks):
	v = new_pageranks[k]
	k = k.strip('\'')		# Not needed again?
	print(k,v,sep=",")
