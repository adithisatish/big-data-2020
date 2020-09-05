#!/usr/bin/python3

import sys

countrycode = None
cur_count = 0
cur_countrycode = None

for line in sys.stdin:
    line = line.strip()
    countrycode,count = line.split("\t",1)
    try:
        count = int(count)
    except ValueError:
        continue

    if cur_countrycode == countrycode:
        cur_count += count
    else:
        if cur_countrycode:
            print(cur_countrycode, cur_count, sep = ",")
        cur_count = count
        cur_countrycode = countrycode

if cur_countrycode==countrycode:
    print(cur_countrycode,cur_count,sep=',')
