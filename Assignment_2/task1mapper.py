#!/usr/bin/python3

#Task 1: Creating Sparse Matrix and Initial Vector from Adjacency List
#Mapper

import sys
import json

for line in sys.stdin:
	line = line.strip()
	line = json.loads(line)

    key = ''
    value = ''


    print(key,value, sep = "\t")

