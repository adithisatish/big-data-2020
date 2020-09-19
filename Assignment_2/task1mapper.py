#!/usr/bin/python3

#Task 1: Creating Sparse Matrix and Initial Vector from Adjacency List
#Mapper

import sys
import json

for line in sys.stdin:
    line = line.strip()

    #Assuming that input file is tab separated and newline delimited

    key,value = line.split(" ",1)

    print(key,value,sep = "\t")