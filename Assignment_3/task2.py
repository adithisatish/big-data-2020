#!usr/bin/python3

import sys
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

searchWord = sys.argv[1]
k = sys.argv[2] # Shouldn't this be converted to int?
pathDataset1 = sys.argv[3]
pathDataset2 = sys.argv[4]

spark = SparkSession.builder.master("local").appName("A3T2").config(conf=SparkConf()).getOrCreate()
# sc = SparkContext(master='local',appName='A3T1')

shape = spark.read.option('header',True).csv(pathDataset1)
shapeStat = spark.read.option('header',True).csv(pathDataset2)

wordmatch1 = shapeStat.filter(shapeStat['word']==searchWord)
wordmatch = wordmatch1.filter(wordmatch1['Total_Strokes']<k)
wordmatch = wordmatch.filter(wordmatch['recognized']==False) # Check if False works or it is 'FALSE' i.e as string and in caps

if wordmatch.collect() != None: # The word was found and there exist records where it is unrecognized and the total strokes of the word is lesser than k
    
    # Non-Copartitioned Join - has to be performed on both word and key_id to avoid repeating columns
    joinedDF = wordmatch.join(shape,on=['key_id','word'],how='inner').groupBy(shape['countrycode']).agg({'Total_Strokes':'count'}).orderBy(shape['countrycode']).collect()

    #countByCountry = joinedDF.groupBy('countrycode').agg({'Total_Strokes':'count'}).collect()

    for i in joinedDF:
        print(i[0],"%.5f"%i[1],sep=',')

else: # What is to be done if any of the conditions fail? I've just printed zero for now
    print(0.00000)

'''
Ouput for 'alarm clock' and 5
FI,78.00000
UA,78.00000
RO,312.00000
PL,78.00000
ZZ,156.00000
EE,78.00000
RU,390.00000
IQ,78.00000
HR,78.00000
CZ,468.00000
NP,78.00000
PT,78.00000
HK,78.00000
TW,156.00000
ID,234.00000
AU,468.00000
SA,312.00000
CA,624.00000
GB,1248.00000
BR,156.00000
DE,936.00000
IL,234.00000
TR,156.00000
ZA,156.00000
CR,78.00000
KR,78.00000
US,5850.00000
RS,78.00000
FR,156.00000
CH,78.00000
GR,78.00000
DJ,78.00000
BA,78.00000
SE,78.00000
PH,390.00000
GE,156.00000
SK,78.00000
TH,234.00000
HU,312.00000
KW,78.00000
IE,234.00000
BE,156.00000
KH,78.00000
NO,156.00000
AR,78.00000
PR,78.00000
VN,78.00000
IS,78.00000
'''    
