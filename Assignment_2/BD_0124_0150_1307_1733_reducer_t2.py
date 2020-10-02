#!/usr/bin/python3

# Input: destination	contribution
# Output: v1: node,new_page_rank

import sys

curr_dest = None
sum_contributions = 0
new_pageranks = {}

for line in sys.stdin:
	line = line.strip()
	dest, cont = line.split('\t',1)
	dest = dest.strip('\'')
	if dest == curr_dest:
		sum_contributions += float(cont)
	else:
		if curr_dest:
			page_rank = 0.85*sum_contributions + 0.15	# No incoming edges -> sum is 0 -> page_rank = 0.15
			new_pageranks[curr_dest] = page_rank
		curr_dest = dest
		sum_contributions = float(cont)
		
page_rank = 0.85*(sum_contributions) + 0.15
new_pageranks[curr_dest] = page_rank


for k in sorted(new_pageranks):
	v = round(new_pageranks[k], 5)
	k = k.strip('\'')		# Not needed again?
	print(k,'%.5f'%v,sep=", ")
