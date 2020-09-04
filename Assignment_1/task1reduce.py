#Task 1 Reducer

import sys

cur_rec = None
cur_count = 0
rec = None

for line in sys.stdin:
    line = line.strip()
    rec,count = line.split("\t",1)
    try:
        count = int(count)
    except ValueError:
        continue

    if cur_rec == rec:
        cur_count += count
    else:
        if cur_rec:
            print(cur_count)
        cur_count = count
        cur_rec = rec

if cur_rec==rec:
    print(cur_count)