''' This script reads rows of the file ../data/name-variants-raw.csv. Each row contains a list of variants of the same
name. It returns a dictionary that for each name (variant), yields a list of other name variants that have appeared
with it in at least one row.''' 


import re
import pprint
import json




pp = pprint.PrettyPrinter(indent=4)

f = open('name-variants-raw.csv', 'r')
fout = open('name-variants.json','w')

content = f.readlines()


# {name:[similars]}
dict_names = {}

index_counter = 0
for line in content:
    line = re.sub(r'\(.*\).+,|-|&|\n|etc|\.|name?s|starting|with|and|ending|male|female|America|meaning|also|name', '', line)
    line = re.sub(r',,', ',', line)
    
    fields = re.split(r',|/', line)
    fields_new = []
    for item in fields:
        if item != '': fields_new.append(item.upper())
    print fields_new
    
    current_group = []
    for name in fields_new:
        if name not in dict_names: dict_names[name] = []
        current_group.append(name)
    for name in fields_new:
        dict_names[name] += current_group
for name in dict_names:
    dict_names[name] = list(set(dict_names[name]))
pp.pprint(dict_names)
    
fout.write(json.dumps(dict_names))
fout.close()
f.close()

print dict_names['MINDY']
