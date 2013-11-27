import json
import pprint

batch_id = 40

pp = pprint.PrettyPrinter(indent=4)
file_adjacency = open('../data/' + str(batch_id) + '-adjacency.json')
file_nodes = open('../data/' + str(batch_id) + '-list_of_nodes.json')


''' Gives a list of links where each link is a list:[source,target]'''
adjacency = json.load(file_adjacency)
dict_nodes = json.load(file_nodes)

# pp.pprint(tmp3)
# quit()


dict_first_names = {}
dict_last_names = {}

for link in adjacency:
    print link
    source = str(link[0])
    target = str(link[1])
    source_last_name,source_first_name,target_last_name,target_first_name='','','',''
    if '1' in dict_nodes[source]['ident_tokens']: source_last_name = dict_nodes[source]['ident_tokens']['1']
    if '2' in dict_nodes[source]['ident_tokens']: source_first_name = dict_nodes[source]['ident_tokens']['2']
    if '1' in dict_nodes[target]['ident_tokens']: target_last_name = dict_nodes[target]['ident_tokens']['1']
    if '2' in dict_nodes[target]['ident_tokens']: target_first_name = dict_nodes[target]['ident_tokens']['2']
    print source_last_name,target_last_name
    if source_first_name not in dict_first_names:
        dict_first_names[source_first_name]=[]
    if source_last_name not in dict_last_names:
        dict_last_names[source_last_name]=[]
   
    if source_last_name!=target_last_name:
        dict_last_names[source_last_name].append(target_last_name)
    if source_first_name!=target_first_name:
        dict_first_names[source_first_name].append(target_first_name)
        
        
pp.pprint(dict_first_names)
    
quit()


for node_id in dict_nodes:
    print '________________________________________________________________________________________________________________________'
    print node_id,dict_nodes[node_id]['ident'];
#     print tmp2[node]['node']
    raw_input()
