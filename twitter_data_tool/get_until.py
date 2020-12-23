#!/usr/bin/env python

from sys import argv

n = eval(argv[1])


with open("twitter_rv.net") as f:
    with open(argv[2], "w") as fout:
        for line in f:
            x, y = map(lambda x: eval(x), line.split('\t'))
            if x > n:
                break
            if y <= n:
                fout.write(line)


