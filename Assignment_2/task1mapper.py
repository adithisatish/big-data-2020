#!/usr/bin/python3

#Task 1: Creating Sparse Matrix and Initial Vector from Adjacency List
#Mapper

import sys
import json

for line in sys.stdin:
    
    try:
        key,value = line.split("\t",1)
        int(key) + 1
    except ValueError:
        continue
        
    line = line.strip()

    #Assuming that input file is space separated and newline delimited

    key,value = line.split(" ",1)

    print(key,value,sep = "\t")
