##################################################################################
# # This script generates a tab-delimited .csv file containing the contribution
# # histories of a random sample of the identified individual donors.
##################################################################################



import json
import pprint
import igraph
import numpy as np
import re
from random  import shuffle
import os



# batch_id = 88; state_id = 'NY'    # New York 
# batch_id = 89; state_id = 'OH'  # Ohio
# batch_id = 90; state_id = 'DE'  # Delaware
# batch_id = 91; state_id = 'MO'   # Missouri
# batch_id = 83; state_id = 'AK'   # Alaska
batch_id = 92; state_id = 'MA'  # Massachussetes
# batch_id = 93; state_id = 'NV'  # Nevada
# batch_id = 94; state_id = 'VT'  # Vermont


 


pp = pprint.PrettyPrinter(indent=4)
data_path = '/home/navid/data/FEC/'
output_path = data_path+'separate_identities/' + state_id + '/'
if not os.path.exists(output_path):
    os.makedirs(output_path)

# data_path = '../results/'
file_adjacency = open(data_path + str(batch_id) + '-adjacency.json')
file_nodes = open(data_path + str(batch_id) + '-list_of_nodes.json')



''' Gives a list of links where each link is a list:[source,target]'''
edgelist = json.load(file_adjacency)
dict_nodes = json.load(file_nodes)
print 'data loaded...'

# pp.pprint(adjacency)
G = igraph.Graph.TupleList(edges=edgelist)

print 'graph generated...'

# # (DIDN'T FIX THE PROBLEM) For some reason the vertex names are automatically assigned and they are floats!
# # I'll convert them to strings, which is what igraph expects later
# for v in G.vs:
#     v['name']=str(int(v['name']))

clustering = G.components()

# The graph of the components: each component is contracted to one node
Gbar = clustering.cluster_graph()
print len(Gbar.vs)

dict_name_2_ind = {}
dict_ind_2_name = {}
employer_adjacency = {}
employer_score = {}
employer_names = []
employer_name_counter = 1
# Loop through the subgraphs, i.e., resolved individual identities.


def bad_employer(employer):    
    if employer == '': return True
    regex = r'\bNA\b|N\.A|employ|self|N\/A|\
                |information request|retired|teacher\b|scientist\b|\
                |applicable|not employed|none|\
                |homemaker|requested|executive|educator\b|\
                |attorney\b|physician|real estate|\
                |student\b|unemployed|professor|refused|doctor|housewife|\
                |at home|president|best effort|consultant\b|\
                |email sent|letter sent|software engineer|CEO|founder|lawyer\b'
    if re.search(regex, employer, flags=re.IGNORECASE): 
        return True
    else: 
        return False
    
list_subgraphs = clustering.subgraphs()
shuffle(list_subgraphs)

count = 0
# print len(list_subgraphs); quit()
for g in list_subgraphs:
    if count > 500: break
   
    list_date_employer = []    
    f_all = open(data_path + str(batch_id) + 'sample.csv', 'w')

    # Loop through the nodes in each subgraph
    for v in g.vs:
        employer = dict_nodes[str(v['name'])]['aux'][1]
        date = dict_nodes[str(v['name'])]['aux'][0]
        name = dict_nodes[str(v['name'])]['ident'][0] + " " + dict_nodes[str(v['name'])]['ident'][1]
        zip = dict_nodes[str(v['name'])]['ident'][2]
        address = dict_nodes[str(v['name'])]['ident'][3]
        if bad_employer(employer): 
#             print employer
            continue
        list_date_employer.append([date, employer, name, zip, address])
    if len(list_date_employer) < 10:
        continue
    else:
        count += 1 
    print count
    list_date_employer = sorted(list_date_employer, key=lambda x:x[0])
    
    
    f = open(output_path +  str(count) + '.csv', 'w')
    for item in list_date_employer:
        f_all.write('%s\t%s\t%s\t%s\t%s\n' % (item[0], item[1], item[2], item[3], item[4]))
        f.write('%s\t%s\t%s\t%s\t%s\n' % (item[0], item[1], item[2], item[3], item[4]))
    f.close()
    f_all.write('########\t########################################\t###########################################\n')
    
f.close()




























