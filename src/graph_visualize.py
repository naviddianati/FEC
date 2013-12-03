import igraph
import numpy as np
from mypalette import MyPalette

def argsort(seq):
    ''' Generic argsort. returns the indices of the sorted sequence'''
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)


batch_id = 70   # New York
# batch_id = 71   # Ohio

output_dim = (3200,3200)

G = igraph.Graph.Load(f='../results/' + str(batch_id) + '-employer_graph.gml', format='gml')
N = len(G.vs)
numcolors = 100

clustering = G.components()

# Only plot the giant component of the graph
# G = clustering.giant()

# Plot the top few largest connected components
all_components = clustering.subgraphs()
all_components_sorted = sorted(all_components, key=lambda graph: len(graph.vs), reverse=True)

list_of_v_ids = []
for g in all_components_sorted[:40]:
    for v in g.vs:
        list_of_v_ids.append(int(v['id']))


print list_of_v_ids
G = G.induced_subgraph(list_of_v_ids)

print G.es[0].attributes()




for v in G.vs:
    v['size'] = np.sqrt(v['size'])

size_max = max(G.vs['size'])
size_min = min(G.vs['size'])
print size_max, size_min
for v in G.vs:
    v['size'] = int(10 + (90 - 10) * (v['size'] - size_min) / (size_max - size_min))


# edgeColors = [int(np.sqrt(x)) for x in G.es['weight']]
edgeWidths = G.es['weight']
x_max, x_min = max(edgeWidths), min(edgeWidths)
print x_max,x_min

edgeWidths = [int(2+(20-2)*(x-x_min)/(x_max-x_min)) for x in edgeWidths]
edgeColors = [int(2+(numcolors-5)*(x-x_min)/(x_max-x_min)) for x in G.es['weight']]
print edgeColors
# quit()
vertexColors = G.vs['size']

# mylayout = G.layout('sugiyama')
mylayout = G.layout('fr', maxiter=1000, maxdelta=2 * N, repulserad=N ** 3)
# mylayout = G.layout('kk',initemp = 10,maxiter = 500)
# mylayout = G.layout('lgl')
# mylayout = G.layout('rt')
# mylayout = G.layout('drl',weights = G.es['weight'])
# mylayout = G.layout('graphopt',node_charge = 50,spring_length=10,spring_constant=10000)
# mylayout = G.layout('mds')
# mylayout = G.layout('star')
# mylayout = G.layout('gfr')

# print G.es.attributes()

# Generate Gradient palette
palettes = []
palettes.append(igraph.AdvancedGradientPalette(["#66ffcc", "#ffffcc", "#ff0066"], n=100));
palettes.append(igraph.AdvancedGradientPalette(["red", "yellow"], n=500));
pal = palettes[0]


pal = igraph.palettes['red-blue']

pal = MyPalette(numcolors)

g = G.copy()
g.es.delete(G.es)

# Plot without edges
p = igraph.plot(g,
    "../results/" + str(batch_id) + "-employers.pdf",
    bbox=output_dim,
    # layout = "large",
    layout=mylayout,
#     vertex_size=5,
    vertex_label=None,
#     ~ vertex_color = colors,
    palette=pal,
    vertex_color=vertexColors,
#     vertex_order_by = ("order","asc"),
    margin=(300, 300, 300, 300),
    opacity=1,
#     background=None,  # This is only possible after my changes to igraph.__init__.py
    background='black'
    # ~ edge_color = edgeColors,
#     edge_color='gray',
#     edge_curved=False,
#     edge_arrow_size = [x/20.0 for x in v_sizes]
    # ~ vertex_label_color = "white",
    
)





# Plot graph with edges
p .add(G,
    bbox=output_dim,
    # layout = "large",
    layout=mylayout,
#     vertex_size=5,
#     vertex_label_size = label_sizes,
#     vertex_label_dist=2,
#     ~ vertex_color = colors,
    palette=pal,
    vertex_color=vertexColors,
    vertex_frame_width=[max(5, x / 10) for x in G.vs['size']],
    vertex_frame_color=vertexColors,
    vertex_label_size=[x*(2.0/3) for x in vertexColors],
    vertex_label_color=vertexColors,
    vertex_label_dist=1.2,
    vertex_order=argsort(vertexColors),
#     vertex_label_color = colors,
#     vertex_order_by = ("order","asc"),
    margin=(300, 300, 300, 300),
    opacity=0.8,
#     background=None,  # This is only possible after my changes to igraph.__init__.py
    background='black',
    edge_color=edgeColors,
#     edge_color='gray',
    edge_curved=False,
    edge_width=edgeWidths

#     edge_arrow_size = [x/20.0 for x in v_sizes]
    # ~ vertex_label_color = "white",
    
)


p.save()

            
