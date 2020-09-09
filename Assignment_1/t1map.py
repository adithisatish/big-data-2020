#!/usr/bin/python3

import sys
import datetime
import json
import re

search_word = sys.argv[1]

def bad_record(record):
	for i in line["word"]:
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


#Mapper
for line in sys.stdin:
	line = line.strip()
	line = json.loads(line)
	
	if line['word']==search_word:
		if bad_record(line) == 0: #if the record is a bad record,ignore
			continue
		rec = None
		count = 1
		date = datetime.datetime.strptime(line['timestamp'],'%Y-%m-%d %H:%M:%S.%f %Z')
		day = date.weekday() #Returns an integer representing the day (o: Monday, 6: Sunday)
        
		if line['recognized']==True:
			rec = "recognized"
		elif (line['recognized']==False and day>=5):
			rec = "unrecognized"
        
		if rec:
			print(rec,count, sep = "\t")
