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

# Checking if the word was not found and appropriately printing zeroes
if len(wordmatch.collect()) == 0:
    print("%.5f" %0.00000,"%.5f" %0.00000,sep='\n')
    
else:
    avgStrokes = wordmatch.groupBy('recognized').agg({"Total_Strokes":'avg'}).orderBy('recognized',ascending=False).collect()

    if (len(avgStrokes)) == 0: # Neither recognized nor unrecognized found - impossible case as specs say clean dataset will be provided, but precautionary measure
        print("%.5f" %0.00000,"%.5f" %0.00000,sep='\n')
    
    else:
        if len(avgStrokes)==1: # Either recognized or unrecognized found

            if avgStrokes[0][0] == 'TRUE': # No unrecognized found - print zero, check if True works or it's 'TRUE' i.e all caps and string
                print("%.5f"%avgStrokes[0][1])
                print("%.5f" %0.00000)
            else: # No recognized found
                print("%.5f" %0.00000)
                print("%.5f"%avgStrokes[0][0])
                
        else: # Both recognized and unrecognized found
            for i in avgStrokes:
                print("%.5f" %i[1])
    
'''
Output for 'alarm clock'
8.26738
11.91667

Output for 'random word'
0.00000
0.00000
'''    
