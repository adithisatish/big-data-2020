#!usr/bin/python3

import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

searchWord = sys.argv[1]
pathDataset1 = sys.argv[2]
pathDataset2 = sys.argv[3]

spark = SparkSession.builder.master("local").appName("A3T1").config(conf=SparkConf()).getOrCreate()
# sc = SparkContext(master='local',appName='A3T1')

shape = spark.read.csv(pathDataset1)
shapeStat = spark.read.csv(pathDataset2)

wordmatch = shapeStat.filter(shapeStat['_c0']==searchWord)

avgStrokes = wordmatch.groupBy('_c2').agg({"_c4":'avg'}).collect()

for i in avgStrokes:
    print(i[1]) 
    #print(i.Total_Strokes)

# Output for 'alarm clock'
# 11.916666...
# 8.26737...

'''
Changes from Task-1.py
1. Column names had to be referenced as (_c0,_c1,...) and not the actual column names. Not sure if this is just in my system.
2. print(i[1]) - this worked for me
3.  Output for word 'alarm clock' : 
     11.916666...
     8.26737.....
'''
