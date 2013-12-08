import igraph
from igraph import color_name_to_rgba
import numpy as np
from mypalettes import gnuplotPalette1, mplPalette
import random

def argsort(seq):
    ''' Generic argsort. returns the indices of the sorted sequence'''
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)


def get_communities(G):
#     c = G.community_infomap()
    c = G.community_label_propagation()
#     c = G.community_edge_betweenness()
    communities = {}
    i = 0
    for g in c.subgraphs():
        communities[i] = tuple([int(x) for x in g.vs['name']])
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
        w = e['weight']
#         tmp = int(w_min+(w_max-w_min)*(w-tmp_min)/(tmp_max-tmp_min))
        if w < threshold:
            death_row.append(e.index)
        else:
            tmp = w_min + (w_max - w_min) * (w - tmp_min) / (tmp_max - tmp_min) 
            e['width'] = tmp
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
            
             

    
def rgba_to_color_name(rgba):
    '''
    @paratm: a 4-tuple of float values in [0,1]
    @return: a string such as "rgba(100,20,220,0.3)"
    '''
    return "rgba(" + ",".join([str(int(255 * rgba[0])), str(int(255 * rgba[1])), str(int(255 * rgba[2])), str(rgba[3])]) + ")"



def set_edgeColor(G, palette):
    numcolor = palette.n
    for e in G.es:
        source = G.vs[e.source]
        target = G.vs[e.target]
        color = color_name_to_rgba(source['color']) if source['size'] > target['size'] else color_name_to_rgba(target['color'])
#         alpha = color * 1.0 / numcolor
#         tmp = palette._get(color)
        e['color'] = rgba_to_color_name((color[0], color[1], color[2], color[3] / 8))
    

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
            color = random.randint(0, numcolor - 1)
            tmp = palette._get(color)
            for v in vs:
                v['color'] = rgba_to_color_name((tmp[0], tmp[1], tmp[2], alpha))
                v['frame_color'] = rgba_to_color_name((tmp[0], tmp[1], tmp[2], alpha / 2))  
#                 print v['color']
                
        return
                       

    tmp_max, tmp_min = max(G.vs[field]), min(G.vs[field])
    for v in G.vs:
        color = int(2 + (numcolor - 1 - 2) * (v[field] - tmp_min) / (tmp_max - tmp_min))
        
        # Get the colors directly from the palette, add alpha and set vertex colors as RGBA tuple
        tmp = palette._get(color)
        alpha = 0.1 + 0.9 * skew(color * 1.0 / numcolor)
        v['color'] = "rgba(" + ",".join([str(int(255 * tmp[0])), str(int(255 * tmp[1])), str(int(255 * tmp[2])), str(alpha)]) + ")"
        v['frame_color'] = rgba_to_color_name((tmp[0], tmp[1], tmp[2], alpha / 2))  

        




def set_vertexSize(G, s_max, s_min, field='size'):
    tmp_max = max(G.vs[field])
    tmp_min = min(G.vs[field])
    for v in G.vs:
        v['size'] = int(s_min + (s_max - s_min) * (v['size'] - tmp_min) / (tmp_max - tmp_min))
    
def set_labelSize(G, s_max, s_min, percent):
    sorted_sizes = sorted(G.vs['size'], reverse=True)
    n = len(G.vs)
    threshold = sorted_sizes[int(n * percent / 100.0)]
    tmp_max = max(G.vs['size'])
    tmp_min = min(G.vs['size'])
    for v in G.vs:
        v['labelSize'] = int(s_min + (s_max - s_min) * (v['size'] - tmp_min) / (tmp_max - tmp_min)) if v['size'] > threshold else 0


# batch_id = 76  # OH
batch_id = 74  # MA
# batch_id = 71   # Ohio
data_path = '/home/navid/tmp/FEC/'

output_dim = (3200, 3200)

# G = igraph.Graph.Load(f=data_path + str(batch_id) + '-employer_graph_giant_component.gml', format='gml')
# G = igraph.Graph.Load(f=data_path + str(batch_id) + '-employer_graph_component-1.gml', format='gml')
G = igraph.Graph.Load(f=data_path + str(batch_id) + '-employer_graph.gml', format='gml')

N = len(G.vs)
numcolors = 100










# Generate Gradient palette
palettes = []
palettes.append(igraph.AdvancedGradientPalette(["#66ffcc", "#ffffcc", "#ff0066"], n=100));
palettes.append(igraph.AdvancedGradientPalette(["red", "yellow"], n=500));
pal = palettes[0]
pal = igraph.palettes['red-blue']
pal = gnuplotPalette1(numcolors)
pal = igraph.ClusterColoringPalette(n=50);pal.n = 50
pal = mplPalette(numcolors, name='gist_ncar')
# pal = mplPalette(numcolors, name='Paired')
# pal = mplPalette(numcolors, name='hot')
pal = mplPalette(numcolors, name='autumn')
# pal = mplPalette(numcolors, name='spring')
# pal = mplPalette(numcolors, name='Set3')

print len(G.vs)



# Set vertex and edge properties
set_vertexSize(G, s_max=30, s_min=2)
set_edgeWidth(G, w_max=30, w_min=1, threshold=5, percent=None)
# print G.es['color']
set_labelSize(G, s_max=20, s_min=1, percent=70)


print len(G.vs)


clustering = G.components()
# Only plot the giant component of the graph
# G = clustering.giant()
# print len(G.vs)
# Plot the top few largest connected components
all_components = clustering.subgraphs()
all_components_sorted = sorted(all_components, key=lambda graph: len(graph.vs), reverse=True)
 
list_of_v_ids = []
for g in all_components_sorted[0:1]:
    print 'component identified ', len(g.vs) 
    vs_index = [v.index for v in G.vs.select(name_in=g.vs['name'])]
    for v in g.vs:
        list_of_v_ids += vs_index
    
# print list_of_v_ids
G = G.induced_subgraph(list_of_v_ids)
# G = all_components_sorted[0]



print 'computing eigenvector centrality'
# evcent = G.evcent(weights='weight')
# evcent = G.evcent()
evcent = G.eccentricity()

for v,i in zip(G.vs,range(len(G.vs))):
    v['evcent'] = evcent[i]

vs = sorted(G.vs,key = lambda v:v['evcent'],reverse = False)
for v in vs:
    print v['label'],v['evcent']

set_vertexColor(G,field = 'evcent', palette=pal, cluster=False)
set_edgeColor(G, palette=pal)


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

mylayout = G.layout('drl', weights=G.es['weight'])
coords = mylayout.coords
mylayout = G.layout('fr', seed=coords, maxiter=100, maxdelta=10)

# mylayout = G.layout('graphopt',node_charge = 50,spring_length=10,spring_constant=10000)
# mylayout = G.layout('mds')
# mylayout = G.layout('star')
# mylayout = G.layout('gfr')

# print G.es.attributes()


g = G.copy()
g.es.delete(G.es)





# Plot graph with edges
p = igraph.plot(G,
    data_path + str(batch_id) + "-employers.pdf",
    bbox=output_dim,
    # layout = "large",
    layout=mylayout,
    vertex_label_size=G.vs['labelSize'],
    vertex_label_color=G.vs['color'],
    vertex_label_dist=1.2,
    
    vertex_frame_width=[max(2, x / 7) for x in G.vs['size']],
    vertex_frame_color=G.vs['frame_color'],
    palette=pal,
    vertex_color=G.vs['color'],
   
#     vertex_label_color = colors,
#     vertex_order_by = ("order","asc"),
    margin=(300, 300, 300, 300),
#     opacity=0.3,
#     background=None,  # This is only possible after my changes to igraph.__init__.py
    background='black',
    edge_color=G.es['color'],
#     edge_color='gray',
    edge_curved=False,
    edge_width=G.es['width']

#     edge_arrow_size = [x/20.0 for x in v_sizes]
    # ~ vertex_label_color = "white",
    
)


# Plot without edges
# p .add(g,
#     bbox=output_dim,
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
#     bbox=output_dim,
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


p.save()

            
