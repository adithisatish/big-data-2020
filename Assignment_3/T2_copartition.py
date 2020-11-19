#!usr/bin/python3

from __future__ import print_function
import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

searchWord = sys.argv[1]
k = int(sys.argv[2]) 
pathDataset1 = sys.argv[3]
pathDataset2 = sys.argv[4]

spark = SparkSession.builder.master("local").appName("A3T2").config(conf=SparkConf()).getOrCreate()
# sc = SparkContext(master='local',appName='A3T1')

shape = spark.read.option('header',True).csv(pathDataset1).repartition('key_id')
shapeStat = spark.read.option('header',True).csv(pathDataset2).repartition('key_id')

wordmatch1 = shapeStat.filter(shapeStat['word']==searchWord)
wordmatch = wordmatch1.filter(wordmatch1['Total_Strokes']<k)
wordmatch = wordmatch.filter(wordmatch['recognized']==False) 

if len(wordmatch.collect()) != 0: # The word was found and there exist records where it is unrecognized and the total strokes of the word is lesser than k
    
    joinedDF = wordmatch.join(shape,on=['key_id','word'],how='inner').groupBy(shape['countrycode']).count().orderBy(shape['countrycode']).collect()

    for i in joinedDF:
        print(i[0],i[1],sep=',')

else: 
    print(0)

'''
Ouput for 'alarm clock' and 5
IE,1
'''
