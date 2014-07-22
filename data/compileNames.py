import json


f = open('name-variants.json')
dict_variants = json.load(f)
f.close()
set_names = set()

for name,variants in dict_variants.iteritems():
    set_names.add(name)
    for other in variants:
        set_names.add(other)

f = open("all-names.json","w")

f.write(json.dumps(list(set_names)))
f.close()
