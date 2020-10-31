#!usr/bin/python3

import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

searchWord = sys.argv[1]
k = sys.argv[2]
pathDataset1 = sys.argv[3]
pathDataset2 = sys.argv[4]

spark = SparkSession.builder.master("local").appName("A3T1").config(conf=SparkConf()).getOrCreate()
# sc = SparkContext(master='local',appName='A3T1')

shape = spark.read.csv(pathDataset1)
shapeStat = spark.read.csv(pathDataset2)

wordmatch1 = shapeStat.filter(shapeStat['_c0']==searchWord)
wordmatch = wordmatch1.filter(wordmatch1['_c4']<k)

# I think this is a copartitioned join, unsure how to change it to non-copartitioned
joinedDF = wordmatch.join(shape,wordmatch['_c0']==shape['_c0'],how='inner').groupBy(shape['_c1']).agg({'_c4':'count'}).collect()

#countByCountry = joinedDF.groupBy('_c0').agg({'_c4':'count'}).collect()

for i in joinedDF:
    print(i[0],i[1],sep=',')
