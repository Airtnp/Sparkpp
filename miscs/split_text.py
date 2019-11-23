#!/usr/bin/python3
import gzip
import json
import os

# ref: https://nijianmo.github.io/amazon/index.html
def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)

def getText(j):
    if 'reviewText' in j.keys():
        return j['reviewText']
    else:
        return ''

l = parse(os.getcwd() + '/Movies_and_TV_5.json.gz')
u = list(l)
v = len(u) // 80

for i in range(80):
    f = open(f"input/input_{i}", "w+")
    d = u[i * v:(i + 1) * v]
    s = map(getText, d)
    f.writelines(s)