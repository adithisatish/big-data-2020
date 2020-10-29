#!usr/bin/python3

import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

searchWord = sys.argv[1]
pathDataset1 = sys.argv[2]
pathDataset2 = sys.argv[3]

spark = SparkSession.builder.master("local").appName("A3T1").config(conf=SparkConf()).getOrCreate()
# sc = SparkContext(master='local',appName='A3T1')

dataset1 = spark.read.csv(pathDataset1)
dataset2 = spark.read.csv(pathDataset2)

wordmatch = dataset1.filter(dataset1['word']==searchWord)

recStrokes = wordmatch.filter(wordmatch['recognized'] == 'TRUE').agg({"Total_Strokes":"avg"})
unrecStrokes = wordmatch.filter(wordmatch['recognized']=='FALSE').agg({"Total_Strokes":"avg"})



