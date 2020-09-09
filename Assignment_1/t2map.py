#!/usr/bin/python3

import sys
import datetime
import json
import re
import math

word = sys.argv[1] #Accepts the word to be searched from the user
k = sys.argv[2]

def bad_record(record): #Checking if the record satisfies predetermined conditions
    
    for i in line["word"]:
        #if i==" " or ord(i) in [97,122] or ord(i) in [65,90]:
        if i.isspace() or i.isalpha():
            continue
        else:
            return 0

    if len(line["countrycode"])!=2 or line["countrycode"].isupper() == False:
        return 0
    if line["recognized"] not in [True, False]:
        return 0
    if len(line["key_id"])!=16 or re.search("^[0-9]*$",line["key_id"])==None:
        return 0
    
    if len(line["drawing"])<1:
        return 0
    else:
        for i in line["drawing"]:
            if len(i) !=2:
                return 0
    
    return 1

def euc_dist(x1,x2,y1,y2):
    return math.sqrt(pow(x2-x1,2)+ pow(y2-y1,2))

#Mapper
for line in sys.stdin:
    line = line.strip()
    line = json.loads(line)

    if line["word"]==word:
        if bad_record(line) == 0: #if the record is a bad record, ignore
            continue
        stroke1 = line["drawing"][0]
        x = stroke1[0][0]
        y = stroke1[1][0]

        dist = euc_dist(0,x,0,y)
        if dist <= float(k):
            continue
        
        country_code = line["countrycode"]
        count = 1
        print(country_code,count,sep = "\t")
