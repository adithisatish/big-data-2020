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

if len(wordmatch.collect()) != 0: # The word was found and there exist records where it is unrecognized and the total strokes of the word is lesser than k
    
    # Non-Copartitioned Join - has to be performed on both word and key_id to avoid repeating columns
    joinedDF = wordmatch.join(shape,on=['key_id','word'],how='inner').groupBy(shape['countrycode']).agg({'Total_Strokes':'count'}).orderBy(shape['countrycode']).collect()

    #countByCountry = joinedDF.groupBy('countrycode').agg({'Total_Strokes':'count'}).collect()

    for i in joinedDF:
        print(i[0],"%.5f"%i[1],sep=',')

else: # What is to be done if any of the conditions fail? I've just printed zero for now
    print("%.5f" %0.00000)

'''
Ouput for 'alarm clock' and 5
AR,8.00000
AU,48.00000
BA,8.00000
BE,16.00000
BR,16.00000
CA,64.00000
CH,8.00000
CR,8.00000
CZ,48.00000
DE,96.00000
DJ,8.00000
EE,8.00000
FI,8.00000
FR,16.00000
GB,128.00000
GE,16.00000
GR,8.00000
HK,8.00000
HR,8.00000
HU,32.00000
ID,24.00000
IE,24.00000
IL,24.00000
IQ,8.00000
IS,8.00000
KH,8.00000
KR,8.00000
KW,8.00000
NO,16.00000
NP,8.00000
PH,40.00000
PL,8.00000
PR,8.00000
PT,8.00000
RO,32.00000
RS,8.00000
RU,40.00000
SA,32.00000
SE,8.00000
SK,8.00000
TH,24.00000
TR,16.00000
TW,16.00000
UA,8.00000
US,600.00000
VN,8.00000
ZA,16.00000
ZZ,16.00000
'''    
