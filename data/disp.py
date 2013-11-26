#! /usr/bin/python
import json
from sys import argv
import pprint

filename = argv[1]
f = open(filename)
data = json.load(f)
pp = pprint.PrettyPrinter(indent=4)
#for item in data:
#    pp.pprint(data[item])
pp.pprint(data)
