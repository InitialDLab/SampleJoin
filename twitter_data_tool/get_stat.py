#!/usr/bin/env python

from sys import argv

def inc_map(m, k):
    if k not in m:
        m[k] = 1
    else:
        m[k] += 1

s = dict()
out_deg = dict()
in_deg = dict()
with open(argv[1]) as f:
    for line in f:
        v = map(lambda x: eval(x), line.split('\t'))
        inc_map(s, v[0])
        inc_map(s, v[1])
        inc_map(out_deg, v[0])
        inc_map(in_deg, v[1])

lst = s.items()
lst = sorted(lst, lambda x, y: y[1] - x[1])
with open(argv[2], "w") as f:
    for x, y in lst:
        f.write("%d %d %d %d\n" % (x, y, in_deg.get(x, 0), out_deg.get(x, 0)))

