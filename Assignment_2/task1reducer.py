#!/usr/bin/python3

#Task 1: Creating Sparse Matrix and Initial Vector from Adjacency List
#Mapper

import sys
import subprocess

cur_key = None

keys = []
values = []

for line in sys.stdin:
    line = line.strip()

    try:
        key,value = line.split("\t",1)
    except Exception as e:
        print("Input Line :",line)
        print("Error :",e)
    

    if cur_key == key:
        values.append(value)
    else:
        keys.append(key)
        
        if cur_key:
            print(cur_key,values,sep = " ") #Not stored in HDFS yet
        
        cur_key = key
        values = [value]
        #values.append(value)# = rec

#Calculating V0:

for i in keys:
    print(key,1)