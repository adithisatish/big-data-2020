import sys
import shutil
import os
import datetime
count=0
n=0
conv =0.5 #this value will vary for different test cases in the backend
epoch = sys.argv[1]
def rewrite_pagerank():
	os.remove("/home/manah/BD/A2/v")

	source = "/home/manah/BD/A2/v1"
	destination = "/home/manah/BD/A2/v"
	dest = shutil.copyfile(source, destination) 



with open("/home/manah/BD/A2/v") as file1, open("/home/manah/BD/A2/v1") as file2, open("/home/manah/BD/A2/log", "a+") as logging:
	for line1, line2 in zip(file1, file2):
		count+=1
		old_pagerank=float(line1.split(",")[1])
		new_pagerank=float(line2.split(",")[1])

		if(abs(old_pagerank-new_pagerank) < conv):
			n+=1

	if(n==count):
		print(0)
	else:
		t = str(datetime.datetime.now())
		logging.write(f"n:{n}\tcount:{count}\t at {t}\n")
		rewrite_pagerank()
		print(1)
