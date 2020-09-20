#!/usr/bin/python3

#Task 1: Creating Sparse Matrix and Initial Vector from Adjacency List
#Mapper

import sys

for line in sys.stdin:
	if(line[0] == '#'):                             #Need to find a better fix, sorry!
		continue
	try:
		line = line.strip()
		source, dest = line.split("\t", 1)
		print(source, dest, sep = "\t")
	except:
		continue

# for line in sys.stdin:
    
#     try:
#         key,value = line.split("\t",1)
#         int(key) + 1
#     except ValueError:
#         continue
        
#     line = line.strip()

#     #Assuming that input file is space separated and newline delimited

#     key,value = line.split(" ",1)

#     print(key,value,sep = "\t")
