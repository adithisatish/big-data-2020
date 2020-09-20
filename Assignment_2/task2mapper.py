import sys

for line in sys.stdin:
    line = line.strip()

    source = line.split(" ")[0]
    destination = (line.split(" ",1)[1]).replace('[','').replace(']',"").split(",")

    contribution = 1/len(destination)

    for i in destination:
        print(i,(source,contribution),sep="\t")