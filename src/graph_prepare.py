import json
import pprint
import igraph
import numpy as np
import re

batch_id = 74   # MA
# batch_id = 75   # NY
# batch_id = 76   # OH

pp = pprint.PrettyPrinter(indent=4)
data_path = '/home/navid/tmp/FEC/'
#data_path = '../results/'
file_adjacency = open(data_path + str(batch_id) + '-adjacency.json')
file_nodes = open(data_path + str(batch_id) + '-list_of_nodes.json')


''' Gives a list of links where each link is a list:[source,target]'''
edgelist = json.load(file_adjacency)
dict_nodes = json.load(file_nodes)

# pp.pprint(adjacency)
G = igraph.Graph.TupleList(edges=edgelist)

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
    if re.search(r'\bNA\b|N\.A|employ|self|N\/A|\
                |information request|retired|\
                |applicable|not employed|none|\
                |homemaker|requested|executive|\
                |attorney|physician|real estate|\
                |student|unemployed|professor|refused|doctor|housewife|\
                |at home|president|best effort|consultant|\
                |email sent|letter sent|software engineer|CEO', employer, flags=re.IGNORECASE): 
        return True
    else: 
        return False
    

for g in clustering.subgraphs():
    list_employers = []
    
    # Loop through the nodes in each subgraph
    for v in g.vs:
        employer = dict_nodes[str(v['name'])]['aux'][1]
        if bad_employer(employer): 
            print employer
            continue
        date = dict_nodes[str(v['name'])]['aux'][0]
        if employer not in dict_name_2_ind : 
            dict_name_2_ind[employer] = employer_name_counter
            employer_name_counter += 1
            dict_ind_2_name[dict_name_2_ind[employer]] = employer
        list_employers.append((dict_name_2_ind[employer], employer))

    # Populate the employer adjacency matrix
    for ind1, name1 in list_employers:
        if ind1 not in employer_score: employer_score[ind1] = 1
        employer_score[ind1] += 0.1
        for ind2, name2 in list_employers:
            if ind1 == ind2:
                continue
                
            link = (ind1, ind2)
            if link not in employer_adjacency:
                employer_adjacency[link] = 1
            else: 
                # This part can change depending on the likelihood function I ultimately decide to use
                employer_adjacency[link] += 1
            

# Generate the graph of linked employer names
tmp_adj = [(x[0], x[1], employer_adjacency[x]) for x in employer_adjacency] 
G_employers = igraph.Graph.TupleList(edges=tmp_adj, edge_attrs=["weight"])

# Set the vertex labels
for v in G_employers.vs:
    v['label'] = dict_ind_2_name[v['name']]





# Set vertex sizes
for v in G_employers.vs:
#     v['size'] = round(np.log(employer_score[v['name']])+10)
    v['size'] = np.sqrt(employer_score[v['name']])




G_employers.save(f=data_path + str(batch_id) + '-employer_graph.gml', format='gml')

clustering = G_employers.components()

subgraphs = sorted(clustering.subgraphs(),key=lambda g:len(g.vs),reverse=True)
for g,i in zip(subgraphs[1:5],range(1,5)):
    print len(g.vs)
    g.save(f=data_path + str(batch_id) + '-employer_graph_component-'+str(i)+'.gml', format='gml')


# save the giant component of the graph
G = clustering.giant()
G.save(f=data_path + str(batch_id) + '-employer_graph_giant_component.gml', format='gml')
# quit()




quit()































dict_first_names = {}
dict_last_names = {}

for link in adjacency:
    # print link
    source = str(link[0])
    target = str(link[1])
    source_last_name, source_first_name, target_last_name, target_first_name = '', '', '', ''
    if '1' in dict_nodes[source]['ident_tokens']: 
        source_last_name = dict_nodes[source]['ident_tokens']['1'] 
    else: 
        print dict_nodes[source]['ident_tokens']
    if '2' in dict_nodes[source]['ident_tokens']: source_first_name = dict_nodes[source]['ident_tokens']['2']
    if '1' in dict_nodes[target]['ident_tokens']: target_last_name = dict_nodes[target]['ident_tokens']['1']
    if '2' in dict_nodes[target]['ident_tokens']: target_first_name = dict_nodes[target]['ident_tokens']['2']
    # print source_last_name,target_last_name
    if source_first_name not in dict_first_names:
        dict_first_names[source_first_name] = []
    if source_last_name not in dict_last_names:
        dict_last_names[source_last_name] = []
        
    if source_last_name != target_last_name:
        dict_last_names[source_last_name].append(target_last_name)
    if source_first_name != target_first_name:
        dict_first_names[source_first_name].append(target_first_name)
        
        
pp.pprint(dict_first_names)
    
quit()

for node_id in dict_nodes:
    print '________________________________________________________________________________________________________________________'
    print node_id, dict_nodes[node_id]['ident'];
#     print tmp2[node]['node']
#    raw_input()
