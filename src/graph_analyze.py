import json
import pprint

batch_id = 15

#13-adjacency.txt
#13-adj_text_auxilliary.json
#13-adj_text_identifiers.json
#13-list_of_identifiers.json
#<F4>13-LSH_hash.txt

        
def get




pp = pprint.PrettyPrinter(indent=4)

file_identifiers = open('../data/'+str(batch_id)+'-list_of_identifiers.json')
file_adj_auxilliary = open('../data/'+str(batch_id)+'-adj_text_auxilliary.json')

file_adj_identifiers = open('../data/'+str(batch_id)+'-adj_text_identifiers.json')

tmp = json.load(file_identifiers)
tmp = json.load(file_adj_auxilliary)
tmp = json.load(file_adj_identifiers)

#pp.pprint(tmp)
#quit()






for node in tmp:
    print '________________________________________________________________________________________________________________________'
    print tmp[node]['node']
    for x in tmp[node]['neighbors']:
        tokens = x[2]
        
        print x[0]
        pp.pprint(tokens)
    raw_input()
