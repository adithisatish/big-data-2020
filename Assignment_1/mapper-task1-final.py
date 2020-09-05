#!/usr/bin/env python

import sys
import pandas as pd
import ndjson

# load from file-like objects
with open('F:/Saurav/Exams/5th Sem/Big Data/Assignments/plane_carriers.ndjson') as f:
    data = ndjson.load(f)


count1 = 0
count2 = 0
for i in data:

    code_attribute = 0
    word_attribute = 0
    recog_attribute = 0
    key_attribute = 0
    drawing_attribute = 0

    if all(x.isalpha() or x.isspace() for x in i["word"]):
        word_attribute = 1

    if(len(i["countrycode"])==2 and i["countrycode"].isupper()):
        code_attribute = 1

    if(isinstance(i["recognized"],(bool))):
        recog_attribute = 1

    if(len(i["key_id"])==16 and i["key_id"].isdigit()):
        key_attribute = 1

    if(len(i["drawing"])>=1 and all(len(x)==2 for x in i["drawing"])):
        drawing_attribute = 1

    if(recog_attribute==1 and word_attribute==1 and code_attribute==1 and key_attribute==1 and drawing_attribute==1):
        if(i["recognized"] and i["word"]==sys.argv[1]):
            count1+=1
            print("recog#",1)

        else:
            #print("else")
            day = pd.Timestamp(i["timestamp"]).weekday()
            day_attribute = 0
            if(day==5 or day==6):
                #print("day")
                day_attribute = 1
            recog = 0
            if(not i["recognized"]):
                recog = 1
                #print("recog")
            if(recog==1 and i["word"]==sys.argv[1]):
                if(day_attribute==1):
                    count2+=1
                    print("unrecog#",1)
#print(count1,count2)
