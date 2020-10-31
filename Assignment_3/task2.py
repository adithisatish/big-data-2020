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

# I think this is a copartitioned join, unsure how to change it to non-copartitioned
joinedDF = wordmatch.join(shape,wordmatch['key_id']==shape['key_id'],how='inner')

countByCountry = joinedDF.groupBy('countrycode').agg({'Total_Strokes':'count'}).collect()

for i in countByCountry:
    print(i.countrycode,i.count,sep=',')