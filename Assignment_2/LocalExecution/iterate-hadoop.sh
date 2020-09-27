#!/bin/sh
CONVERGE=1
rm v* log*

bin/hadoop dfsadmin -safemode leave
bin/hdfs dfs -rm -r /Output/output* 

bin/hadoop jar /home/manah/Hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
-mapper "/home/manah/BD/A2/t1map.py " \
-reducer "/home/manah/BD/A2/t1reduce.py '/home/manah/BD/A2/v'"  \
-input /Input/dataset-A2 \
-output /Output/output1 #has adjacency list


while [ "$CONVERGE" -ne 0 ]
do
	bin/hadoop jar /home/manah/Hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
	-mapper "/home/manah/BD/A2/t2map.py '/home/manah/BD/A2/v' " \
	-reducer "/home/manah/BD/A2/t2reduce.py " \
	-input /Output/output1 \
	-output /Output/output2
	touch v1
	bin/hadoop fs -cat /Output/output2/* > /home/manah/BD/A2/v1
	CONVERGE=$(python3 check_conv.py >&1)
	bin/hdfs dfs -rm -r /Output/output2
	echo $CONVERGE

done
