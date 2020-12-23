#!/usr/bin/env python

from sys import argv

c = set()
with open("celebrities_id.txt") as f:
    for line in f:
        id = eval(line)
        c.add(id)

with open(argv[1]) as f:
    with open(argv[2]) as fout:
        for line in argv[1]:
            x, y = map(lambda x: eval(x), line.split('\t'))
            if x in c:
                fout.write(x)
    
