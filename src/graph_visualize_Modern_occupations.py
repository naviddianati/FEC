

import igraph
from igraph import color_name_to_rgba
import numpy as np
from mypalettes import gnuplotPalette1, mplPalette
import random
import json
import re
import os
import matplotlib.pyplot as plt

class Settings:
    def __init__(self, draw_groups=False, batch_id=94, state_id='VT',
                 affiliation='occupation', data_path = os.path.expanduser('~/data/FEC/'),
                 output_dim=(3200, 3200), num_components=(0, 80), verbose=False):
        self.draw_groups = draw_groups
        self.batch_id = batch_id
        self.state_id = state_id
        self.affiliation = affiliation
        self.data_path = data_path
        self.output_dim = output_dim
        self.num_components = num_components
        self.verbose = verbose
        
        
        


def argsort(seq):
    ''' Generic argsort. returns the indices of the sorted sequence'''
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)


def get_communities(G):
#     c = G.community_infomap()
    c = G.community_label_propagation()
#     c = G.community_leading_eigenvector()
#     c = G.community_spinglass()

    communities = {}
    i = 0
    for g in c.subgraphs():
        communities[i] = tuple([x for x in g.vs['name']])
        i += 1
    return communities
    




def set_edgeWidth(G, w_max, w_min, field='weight', threshold=None, percent=100):
    if not threshold:
        if percent:
            sorted_sizes = sorted(G.es[field], reverse=True)
            n = len(G.es)
            threshold = sorted_sizes[int(n * percent / 100.0)]
        else: 
            threshold = 0
    
    print "edgeWidth threshold: ", threshold
    values = G.es[field]
    tmp_max, tmp_min = max(values), min(values)
    
    # Threshold the widths AND the weights themselves (to influence the layout)
    death_row = []
    for e in G.es:
        w = e[field]
#         tmp = int(w_min+(w_max-w_min)*(w-tmp_min)/(tmp_max-tmp_min))
        if w < threshold:
            death_row.append(e.index)
            continue
        else:
            tmp = w_min + (w_max - w_min) * (w - tmp_min) / (tmp_max - tmp_min) 
            e['width'] = tmp
        e['size'] = e['width']  # for redering with sigma.js
    G.delete_edges(death_row)
    
    # weed out vertices with no links
    death_row = []
    for v in G.vs:
        edge_weights = []
        for u in v.neighbors():
            edges = G.es.select(_between=([u.index], [v.index]))
            edge_weights += edges[field]
            
        if not edge_weights:
            death_row.append(v.index)
#             G.delete_vertices(v)
            continue
#         print edge_weights
        if max(edge_weights) < threshold:
            death_row.append(v.index)

#             print 'vertex deleted'
    G.delete_vertices(death_row)
#             v['size'] = 0
            
             

    
#===============================================================================
# rgba_to_color_name
#===============================================================================
def rgba_to_color_name(rgba):
    '''
    @paratm: a 4-tuple of float values in [0,1]
    @return: a string such as "rgba(100,20,220,0.3)"
    '''
    return "rgba(" + ",".join([str(int(255 * rgba[0])), str(int(255 * rgba[1])), str(int(255 * rgba[2])), '%0.2f' % rgba[3]]) + ")"
#     return "rgb(" + ",".join([str(int(255 * rgba[0])), str(int(255 * rgba[1])), str(int(255 * rgba[2]))]) + ")"


def dim_color(rgba):
    dimmed_rgba = [(1 * x + 2 * 1.0) / 3 for x in rgba[:3]]
    dimmed_rgba.append(rgba[3] * 0.8)
    return dimmed_rgba
#===============================================================================
# set_edgeColor
#===============================================================================
def set_edgeColor(G, palette, field=None):
    numcolor = palette.n
    if field is not None:
        tmp_max = max(G.es[field])
        tmp_min = min(G.es[field])
         
    for e in G.es:
        source = G.vs[e.source]
        target = G.vs[e.target]
        if field is None:
            color_str = source['color'] if source['size'] > target['size'] else target['color']
            color = color_name_to_rgba(color_str)
#             color = source['color'] if source['size'] > target['size'] else target['color']
            alpha = color[3]
        else:
            color_int = int(2 + (numcolor - 1 - 2) * (e[field] - tmp_min) / (tmp_max - tmp_min))
#             print color_int
            color = palette._get(color_int) 
            alpha = float(color_int) / numcolor
#         alpha = color * 1.0 / numcolor
#         tmp = palette._get(color)

        e['color'] = rgba_to_color_name(dim_color(color))
#         print color_str, color, e['color']
    

def skew(x):
    return np.sin(np.sqrt(x) * np.pi / 2)

def set_vertexColor(G, palette, field='size', cluster=False):
    numcolor = palette.n
    if cluster:
        list_clouts = []
        dict_clouts = {}
        communities = get_communities(G)
        for c in communities:
            vs = [G.vs.find(name=nm) for nm in communities[c]]
            clout = sum([v['size'] for v in vs])
            list_clouts.append(clout)
            dict_clouts[c] = clout
        clout_max = max(list_clouts)
        clout_min = min(list_clouts)
        for c in communities:
            clout = dict_clouts[c]
            alpha = 0.1 + 0.9 * skew((clout - clout_min) * 1.0 / (clout_max - clout_min))
            vs = [G.vs.find(name=nm) for nm in communities[c]]
            color = random.randint(0, int(numcolor * 0.7))
            tmp = palette._get(color)
            for v in vs:
                v['color'] = rgba_to_color_name((tmp[0], tmp[1], tmp[2], alpha))
                v['frame_color'] = rgba_to_color_name((tmp[0], tmp[1], tmp[2], alpha / 2))  
#                 print v['color']
                
        return
                       
    
    tmp_max, tmp_min = max(G.vs[field]), min(G.vs[field])
    for v in G.vs:
        color = int(60 + (numcolor - 1 - 60) * (v[field] - tmp_min) / (tmp_max - tmp_min))
        
        # Get the colors directly from the palette, add alpha and set vertex colors as RGBA tuple
        tmp = palette._get(color)
        alpha = 0.1 + 0.9 * skew(color * 1.0 / numcolor)
        v['color'] = "rgba(" + ",".join([str(int(255 * tmp[0])), str(int(255 * tmp[1])), str(int(255 * tmp[2])), str(alpha)]) + ")"
        v['frame_color'] = rgba_to_color_name((tmp[0], tmp[1], tmp[2], 1.0))  

        




def set_vertexSize(G, s_max, s_min, field='size'):
    tmp_max = max(G.vs[field])
    tmp_min = min(G.vs[field])
    for v in G.vs:
        v['size'] = int(s_min + (s_max - s_min) * (v['size'] - tmp_min) / (tmp_max - tmp_min))
    
def set_labelSize(G, s_max, s_min, percent):
    sorted_sizes = sorted(G.vs['size'], reverse=True)
    n = len(G.vs)
    if percent is None:
        threshold = min(sorted_sizes) - 1
        print " vertex size threshold: None"
    else:
        threshold = sorted_sizes[int(n * percent / 100.0)]
    print 'Threshold:', threshold  
    tmp_max = max(G.vs['size'])
    tmp_min = min(G.vs['size'])
    for v in G.vs:
        v['labelSize'] = int(s_min + (s_max - s_min) * (v['size'] - tmp_min) / (tmp_max - tmp_min)) if v['size'] > threshold else 0
        
        
        
        
''' This method converts an igraph Graph object into the JSON format understood by Sigma'''
def graph_to_json(G, layout=None):    
    g = {'nodes':[], 'edges':[]}
    node_attributes = ['size', 'label', 'color', 'labels-all']
    edge_attributes = [ 'color', 'weight', 'confidence', 'width', 'size']
    emax = 0
    vmax = 0
    for v in G.vs:
        print v.index
        node = {}
        if vmax < v['id']: vmax = v['id']
        for attr in node_attributes:
            if attr in v.attribute_names():
                    node[attr] = v[attr]
        node['id'] = '%d' % v.index      
        if layout is None:
            x, y = random.random(), random.random()
        else:
            x, y = layout[v.index]
        node['x'], node['y'] = x, y
        g['nodes'].append(node)
    for i, e in enumerate(G.es):
        if emax < e.source : emax = e.source
        if emax < e.target : emax = e.target
        edge = {'source':'%d' % e.source, 'target':'%d' % e.target}
        for attr in edge_attributes:
            if attr in e.attribute_names():
                edge[attr] = e[attr]
        edge['id'] = '%d' % i
#         edge['color'] = 'rgb(200,200,200)'

        g['edges'].append(edge)
    print 'vmax: ', vmax, '   emax: ', emax
    return g
 
 
def set_group_markers(G, membership_vector, palette):
    ''' Given a graph and a membership_vector (list), this functions computes a dictionary
    {(indices):color} that can be passed Graph.plot() 's mark_groups property.
    Also, a new edge "weight" property is added that vastly distinguishes inter-cluster
    edges from intra-cluster edges to assist in a community-respecting layout.'''
    N = len(membership_vector)
    n = len(set(membership_vector))
    numcolor = palette.n

    dict_markers = {}
    for  i in range(n):
        tuple_indices = tuple([j for j in range(N) if membership_vector[j] == i])
        color = random.randint(int(numcolor * 0.5), int(numcolor * 0.7))
        tmp = palette._get(color)
        dict_markers[tuple_indices] = rgba_to_color_name((tmp[0], tmp[1], tmp[2], 0.7)) 
    G['group-markers'] = dict_markers  
     
    for e in G.es:
        e['layout-weight'] = 1
    for group_indices in G['group-markers']:
        
        subgraph = G.subgraph(group_indices)
        for e in subgraph.es:
            G.es[int(e['id'])]['layout-weight'] = 10
            
            
 
     
   
#========================================================================
# set of boolean functions deciding whether two names are "identical"
#========================================================================
def identity_function_size(v1, v2):
    return True if v1['size'] < v2['size'] else False

def identity_function_confidence(e):
    threshold = 0.95
    print e['confidence']
    return True if e['confidence'] > threshold else False

def identity_function_common_word(v1, v2):
    label1 = re.sub(r'\,|\+|\*|\||\(|\)|\/', ' ', v1['label'][0])
    label1 = re.sub(r'\.|\?', '', label1)
    
    regex_list = [x for x in label1.split() if len(x) > 1]
    regex = r'\b' + r'\b|\b'.join(regex_list) + r'\b'
    print regex
#     print  v1['label'][0],'______________', v2['label'][0]
    return True if re.search(regex, v2['label'][0]) else False
        
       

def collapse_graph(G, identity_function, comparison_mode='vertex'):
    ''' This function returns a collapsed version of G by contracting some edges.
    the function identity_function should be able to take two vertices as arguments
    and return True of False indicating whether the two should be considered identical.'''
    H = G.copy()
    pointers = {v.index:v.index for v in G.vs}

    edge_death_row = []
    for e in H.es:
        i0, i1 = e.source, e.target
        
        if comparison_mode == 'vertex':
            should_collapse = identity_function(H.vs[i0], H.vs[i1])
        if comparison_mode == 'edge':
            should_collapse = identity_function(e)
            
        if should_collapse:  # set source pointer to target id
            if settings.verbose: print "SAME: ", H.vs[i0]['label'][0], '------------------', H.vs[i1]['label'][0], '\n'
            pointers[i0] = pointers[i1]
            print H.vs[i0]['label'][0], '==================', H.vs[i1]['label'][0]
        else: 
#             e.delete()
            edge_death_row.append(e.index)
            print H.vs[i0]['label'][0], '888888888888888888', H.vs[i1]['label'][0]
    
    H.delete_edges(edge_death_row)
    if settings.verbose:
        print "no of edges of G: ", len(G.es)
        print "no of edges of H: ", len(H.es)
    c = H.components()
    
    print len(H.vs)
    my_map = [0 for i in xrange(len(H.vs))]
    for i, subgraph in enumerate(c.subgraphs()):
#         print i
        for v in subgraph.vs:
#             print len(H.vs)
            my_map[int(v['id'])] = i
    print my_map
#     quit()
            
#     This is no longer necessary
#     counter = 0
#     id  = 0
#     map_new = []
#     all_indices = {}
#     for i,x in enumerate(map):
#         if map[i] not in all_indices:
#             id = counter
#             counter +=1
#         else:
#             id = all_indices[map[i]]
#         map_new.append(id)
#         all_indices[map[i]] = id
    

#     my_map = [i for i in xrange(len(G.vs))]  # null collapse
    G.contract_vertices(my_map, combine_attrs=dict_combine_attrs)
    return G
     

def make_dendrogram(G):
    G.simplify(combine_edges='mean')
#     dendr = G.community_walktrap()
    dendr = G.community_edge_betweenness(weights='confidence')
#     G.membership_vector = dendr.as_clustering(n=dendr.optimal_count).membership
    G.vs['membership_vector'] = dendr.as_clustering(n=dendr.optimal_count).membership
#     G.membership_vector = dendr.as_clustering(n=40).membership
    print "optimal cluster count:", dendr.optimal_count
    igraph.plot(dendr, 'navid.pdf', bbox=(3200, 9200))
    
     
def remove_loops(G):
    for e in G.es:
        if e.target == e.source:
            if settings.verbose:
                print e.target, e.source
                print 'edge deleted'
            e.delete()
    
def normalize_labels(G):
    for v in G.vs:
        v['label'] = v['label'][0]
    


def print_sorted_significance(n = 100):
    # print edges with top significnce values
    sig_edges = sorted(G.es, key=lambda e:e['confidence'], reverse=False)
    for e in sig_edges[:n]:
        n0,n1 = e.source,e.target
        print G.vs[n0]['label'], "-"*10, G.vs[n1]['label'], e['confidence']
#         print G.vs[n0], G.vs[n1], e['confidence']

settings = Settings(batch_id=94,
                    state_id='VT',
                    affiliation='employer',
#                     affiliation='occupation',
                    data_path = os.path.expanduser('~/data/FEC/'),
                    output_dim=(3200, 3200),
                    num_components=(0, 1),
                    verbose=False
                     )
      

#     print G.vs['labelSize']
#     quit()

# settings.state='delaware'; settings.state_id = 'DE'  
settings.state='newyork'; settings.state_id = 'NY'  
# settings.batch_id = 90; settings.state_id = 'DE'  # Delaware
# settings.batch_id = 91; settings.state_id = 'MO'   # Missouri
# settings.batch_id = 83; settings.state_id = 'AK'   # Alaska
# settings.batch_id = 92; settings.state_id = 'MA'  # Massachussetes
# settings.batch_id = 93; settings.state_id = 'NV'  # Nevada
# settings.batch_id = 94; settings.state_id = 'VT'  # Vermont





''' This dictionary defines how the new vertex attribute is to be computed when some vertices are contracted'''
dict_combine_attrs = {'size':lambda sizes:np.sqrt(np.sum([np.sqrt(x) for x in sizes])),
                    'labels-all':lambda mylist: ''.join([x + '<br/>' for x in mylist]),
                      'label' :lambda mylist: mylist[np.argmax([x[1] for x in mylist])][0],
                      'name':lambda mylist: ''.join([x + '\n' for x in mylist]),
                      'id': np.min
                      }
# G = igraph.Graph.Load(f=settings.data_path + str(settings.state) + '-' + settings.affiliation + '_graph_giant_component.gml', format='gml')

# print [v['label'] for v in G.vs[1].neighbors()]
# quit()

# G = igraph.Graph.Load(f=settings.data_path + settings.state + '-' + settings.affiliation + '_graph_component-1.gml', format='gml')
G = igraph.Graph.Load(f=settings.data_path + str(settings.state) + '-' + settings.affiliation + '_graph.gml', format='gml')



a = [float(x) for x in G.es['confidence']]


print_sorted_significance()
# quit()
print sorted(a)
# plt.plot(sorted(a), '.')
# plt.show()
# quit()

def get_neighborhood(index,n):
    if n == -1: return None
    neighbors = G.vs[index].neighbors()
    list_ids = [v.index for v in neighbors]
    list_ids.append(index)
    for i,v in enumerate(neighbors):
        
        new_ones = get_neighborhood(v.index,n-1)
        if new_ones:
            list_ids += new_ones 
    return list(set(list_ids))    
    
        
# Optionally select a neighborhood of a given vertex
target_label = 'UNIVERSITY OF DELAWARE'
v = G.vs.select(label=target_label)[0]
print v
list_indexes =  get_neighborhood(v.index,0)
print len(list_indexes)
G = G.induced_subgraph(list_indexes)



G.vs['membership_vector'] = None
G['group-markers'] = None
for v in G.vs:
    v['label'] = (v['label'], v['size'])
    v['labels-all'] = v['label'][0]
    
print len(G.vs)
# G = collapse_graph(G, identity_function_common_word)
# G = collapse_graph(G, identity_function_confidence, comparison_mode='edge')
normalize_labels(G)
print len(G.vs)
print G.vs.attribute_names()
# G.simplify(loops=True, combine_edges='sum')
print G.vs.attribute_names()
N = len(G.vs)
numcolors = 100

print G.es.attribute_names()
print G.es['weight']
# quit()
# make_dendrogram(G)

G.es['id'] = [e.index for e in G.es]

print N


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

def get_top_identifiers(G, field='size', n=200, batch_size=50):
    ordered_list = [[v.index, v['size']] for v in G.vs]
    ordered_list = sorted(ordered_list, key=lambda x:float(x[1]), reverse=True)
    n = min(n, len(G.vs))
    i = 0
    output_path = settings.data_path+settings.affiliation+"-tagging/"+settings.state_id
    if not os.path.exists(output_path):
        os.makedirs(output_path)
     
    for batch_no, sublist in enumerate(chunks(ordered_list, batch_size)):
        filename = output_path +"/%d.json" % batch_no
        dict_identifiers = {i:{"label":G.vs[ordered_list[i][0]]['label'],
                               "neighbors":[v['label'] for v in sorted(G.vs[ordered_list[i][0]].neighbors(),key=lambda v:v['size'])[:20]   ]} 
                            for i in xrange(len(sublist)) }
        f = open(filename,'w')
        f.write(json.dumps(dict_identifiers,indent=4))
        f.close()
    
     

get_top_identifiers(G, field='size', n=1000)
# quit()




# Generate Gradient palette
palettes = []
palettes.append(igraph.AdvancedGradientPalette(["#66ffcc", "#ffffcc", "#ff0066"], n=100));
palettes.append(igraph.AdvancedGradientPalette(["red", "yellow"], n=500));
pal = palettes[0]
pal = igraph.palettes['red-blue']
pal = gnuplotPalette1(numcolors)
pal = igraph.ClusterColoringPalette(n=50);pal.n = 50
# pal = mplPalette(numcolors, name='gist_ncar')
# pal = mplPalette(numcolors, name='Paired')
# pal = mplPalette(numcolors, name='hot')
# pal = mplPalette(numcolors, name='autumn')
# pal = mplPalette(numcolors, name='spring')
# pal = mplPalette(numcolors, name='Set1')
# pal = mplPalette(numcolors, name='jet')
# pal = mplPalette(numcolors, name='PuOr')
# pal = mplPalette(numcolors, name='bone')
pal = mplPalette(numcolors, name='binary')
# pal = mplPalette(numcolors, name='gray')
# pal = mplPalette(numcolors, name='spectral')

print len(G.vs)




# Set vertex and edge properties
set_vertexSize(G, s_max=80, s_min=10)
set_edgeWidth(G, w_max=20, w_min=1, field = 'confidence', threshold=None, percent=50)
# print G.es['color']
set_labelSize(G, s_max=80, s_min=20, percent=None)



if G.vs['membership_vector']:
    set_group_markers(G, G.vs['membership_vector'], palette=pal)

# set_group_markers(G, G.vs['membership_vector'], palette=pal)





print len(G.vs)

filter_components = False

if filter_components:
    clustering = G.components()
    # Only plot the giant component of the graph
    # G = clustering.giant()
    # print len(G.vs)
    # Plot the top few largest connected components
    all_components = clustering.subgraphs()
    all_components_sorted = sorted(all_components, key=lambda graph: len(graph.vs), reverse=True)
    
    list_of_v_ids = []
    for g in all_components_sorted[settings.num_components[0]:settings.num_components[1]]:
        print 'component identified ', len(g.vs) 
        vs_index = [v.index for v in G.vs.select(name_in=g.vs['name'])]
        for v in g.vs:
            list_of_v_ids += vs_index
        
    print "number of components: ", len(all_components)
    # print list_of_v_ids
    G = G.induced_subgraph(list_of_v_ids)
    # G = all_components_sorted[0]



print 'computing eigenvector centrality'
# evcent = G.evcent(weights='weight')
# evcent = G.evcent()
evcent = G.eccentricity()

for v, i in zip(G.vs, range(len(G.vs))):
    v['evcent'] = evcent[i]
    v['random'] = random.randint(0, numcolors - 1)
    v['degree'] = v.degree()
    
vs = sorted(G.vs, key=lambda v:v['evcent'], reverse=False)

print G.es['confidence']


print "Hello"
# G.es['betweenness'] = [np.power(x, 3) for x in G.edge_betweenness(weights=G.es['confidence'])]
print "Hello"


print "______________"
for v in G.vs:
    print v['size'],"----"
print G.vs['size']
print "______________"
# quit()



# set_vertexColor(G, field='membership_vector', palette=pal, cluster=False)
set_vertexColor(G, palette=pal, cluster=False)
set_edgeColor(G, palette=pal, field=None)


print len(G.vs)
# clustering = G.components()
# all_components = clustering.subgraphs()
# print len(all_components)
# quit()


# quit()
# mylayout = G.layout('sugiyama')
# mylayout = G.layout('fr', maxiter=1000, maxdelta=2 * N, repulserad=N ** 3)
# mylayout = G.layout('fr')
# mylayout = G.layout('kk', initemp=10, maxiter=500)
# mylayout = G.layout('lgl')
# mylayout = G.layout('rt')

# mylayout = G.layout('drl', weights=G.es['betweenness'])
# mylayout = G.layout('drl')
mylayout = G.layout('drl', weights=G.es['layout-weight'])

coords = mylayout.coords

# mylayout = G.layout('fr', seed=coords, maxiter=100, maxdelta=1,weights=np.sqrt(G.es['layout-weight']),repulserad=0)
mylayout = G.layout('fr', seed=coords, maxiter=1000, maxdelta=10)
print "Layout computed..."



# my_dist = np.ones([len(G.vs),len(G.vs)])*100.0
# for e in G.es:
#     ind0,ind1 = e.source,e.target
#     my_dist[ind0,ind1] = my_dist[ind1,ind0] = e['layout-weight']
#     if e['layout-weight'] <20: print ind1,ind0
# mylayout = G.layout('mds',dist=my_dist)
#     



# filename_tmp = settings.data_path + str(settings.batch_id) + "-graph-" + settings.affiliation + "s.json"
filename_tmp = "/home/navid/Dropbox/FEC-data/results/" + settings.affiliation + "s-" + settings.state_id + ".json"
f = open(filename_tmp, 'w')
f.write(json.dumps(graph_to_json(G, mylayout)))
f.close()
print "Graph saved in JSON format..."


def distance(x1, y1, x2, y2):
    return np.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)

def energy(data):
    N = len(data)
    e = 0
    for i in xrange(N - 1):
        for j in xrange(i + 1, N):
            tmp = distance(data[i][0][0], data[i][0][1], data[j][0][0], data[j][0][1]) - data[i][1] - data[j][1]
            e += -tmp if tmp < 0 else 0
    return e


def noverlap(G, mylayout):
    data = []
    vs = G.vs
    N = len(mylayout)
    for i in xrange(N):
        data.append([mylayout[i], vs[i]['size']])

    e_new = energy(data)
    for i in xrange(100):
        n = random.randint(0, N - 1)
        dx = random.random()
        dy = random.random()
        e = e_new
        data[n][0][0] += dx
        data[n][0][1] += dy
        e_new = energy(data) 
        if e < e_new:
            if random.random() > 0.1:
                data[n][0][0] -= dx
                data[n][0][1] -= dy
                e_new = e
        print i       
    return [x[0] for x in data]
  
    


# mylayout_noverlap = noverlap(G, mylayout)

# quit()

# mylayout = G.layout('graphopt',node_charge = 50,spring_length=10,spring_constant=10000)
# mylayout = G.layout('mds')
# mylayout = G.layout('star')
# mylayout = G.layout('gfr')

# print G.es.attributes()

print G['group-markers']
# quit()


g = G.copy()
g.es.delete(G.es)


leaves_seq = G.vs.select(_degree_le=0)
leaves_seq.delete()


# Plot graph with edges
p = igraph.plot(G,
    settings.data_path + str(settings.batch_id) + "-" + settings.affiliation + "s.pdf",
    bbox=settings.output_dim,
    # layout = "large",
    layout=mylayout,
    vertex_label_size=G.vs['labelSize'],
    vertex_label_color=G.vs['color'],
#     vertex_label_color='#000000',
    vertex_label_dist=1.2,
    
    vertex_frame_width=[max(2, x / 7) for x in G.vs['size']],
    vertex_frame_color=G.vs['frame_color'],
    palette=pal,
    vertex_color=G.vs['color'],
    mark_groups=G['group-markers'] if G['group-markers'] and settings.draw_groups else None,
#     vertex_label_color = colors,
#     vertex_order_by = ("order","asc"),
    margin=(300, 300, 300, 300),
#     opacity=0.3,
#     background=None,  # This is only possible after my changes to igraph.__init__.py
    # background='#050505',
    background='#fff',
    edge_color=G.es['color'],
#     edge_color='gray',
    edge_curved=False,
    edge_width=G.es['width']

#     edge_arrow_size = [x/20.0 for x in v_sizes]
    # ~ vertex_label_color = "white",
    
)


# Plot without edges
# p .add(g,
#     bbox=settings.draw_groups,
#     # layout = "large",
#     layout=mylayout,
# #     vertex_size=5,
#     vertex_label=None,
# 
# #     ~ vertex_color = colors,
#     palette=pal,
#     vertex_color=g.vs['color'],
# #     vertex_order_by = ("order","asc"),
#     margin=(300, 300, 300, 300),
#     opacity=1,
# #     background=None,  # This is only possible after my changes to igraph.__init__.py
#     background='black'
#     # ~ edge_color = edgeColors,
# #     edge_color='gray',
# #     edge_curved=False,
# #     edge_arrow_size = [x/20.0 for x in v_sizes]
#     # ~ vertex_label_color = "white",
#     
# )


# 
# p.add(g,
#     bbox=settings.draw_groups,
#     # layout = "large",
#     layout=mylayout,
# #     vertex_size=5,
# #     ~ vertex_color = colors,
#     palette=pal,
#     vertex_color=g.vs['color'],
#     vertex_order=argsort(G.vs['size']),
#     
#     vertex_frame_width=[max(2, x / 10) for x in G.vs['size']],
#     vertex_frame_color=G.vs['color'],
#     vertex_label=None,
#     opacity=0.5,
# #     vertex_order_by = ("order","asc"),
#     margin=(300, 300, 300, 300),
# #     opacity=1,
# #     background=None,  # This is only possible after my changes to igraph.__init__.py
#     background='black'
#     
# )


# p.save()

            
