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
shapeStat = spark.read.option('header',True).csv(pathDataset2)

wordmatch = shapeStat.filter(shapeStat['word']==searchWord)

avgStrokes = wordmatch.groupBy('recognized').agg({"Total_Strokes":'avg'}).collect()

for i in avgStrokes:
    print("%.5f" %i[1])
    
'''
Output for 'alarm clock'
11.91667
8.26738
'''    
