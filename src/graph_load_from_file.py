''' This is the Ngram version! '''

import igraph as ig
from numpy import array
from pylab import *
from igraph.drawing.utils import BoundingBox




def myplot(obj,target=None, *args, **kwds):
    bkgnd  = None
    #~ target = None
    bbox=(0, 0, 600, 600)
    if not isinstance(bbox, BoundingBox):
        bbox = BoundingBox(bbox)
   
    if "canvas" not in kwds:
        print "generating new Plot instance..."
        canvas =ig.Plot(target, bbox, background=bkgnd)
    else: 
        print "adding to existing Plot instance..."
        canvas = kwds["canvas"]

    bbox = bbox.contract(20)
    print "adding object to canvas..."
    canvas.add(obj, bbox, *args, **kwds)

    if ("draw" in kwds) and (kwds["draw"] == True):
        print "drawing / saving..."
        if target is None:
            canvas.show()

        if isinstance(target, basestring):
            canvas.save()

    return canvas





#~ p = ig.Plot("test.svg", background = None)

colornum = 100

# Read graph from .mtx file containing edge list
#~ h = ig.Graph.Read_Ncol("/home/navid/Dropbox/Project5/matrices/football.mtx", directed = False)
#~ h = ig.read("/home/navid/Dropbox/Project5/matrices/adjnoun.gml")
h = ig.read("../data/adjacency.txt", format="edgelist")
print len(h.vs)
# h = h.as_undirected()
# set layout
mylayout = "mds"


# Generate Gradient palette
pal = ig.AdvancedGradientPalette(["black","red","yellow"],n = 100);

values =  [int(v.betweenness()*10000) for v in h.vs]
#~ values = [int(v.betweenness()*2) for v in h.vs]

cmax, cmin = max(values), min(values)
if cmax == cmin: cmax = cmin+1
colors = [int((cmax - x)*1.0/(cmax-cmin)*(colornum-21)) for x in values]

v_sizes = [int((x - cmin)*1.0/(cmax-cmin)*(5))+8 for x in values]
label_sizes = [int((x - cmin)*1.0/(cmax-cmin)*(20))+12 for x in values]

values = [values[x.target] * values[x.source] for x in h.es]
ecmax,ecmin = max(values), min(values)
if ecmax == ecmin: ecmax = ecmin+1
edgeColors = [int((ecmax - x)*1.0/(ecmax-ecmin)*(colornum-1)) for x in values]

edgeWidths = [ceil((x-ecmin)*1.0/(ecmax-ecmin)*(4)+0.01) for x in values]
print edgeWidths


# Add an attribute to the vertex sequence of the graph
h.vs["order"] = v_sizes


mylayout = h.layout('fr')
mylayout = h.layout('fr')
mylayout = h.layout('fr')
mylayout = h.layout('fr')





# Plot graph with edges
p = ig.plot(h,
    "../data/adjnoun-vertices-edges.pdf",
    bbox = (1800,1800),
    #layout = "large",
    layout = mylayout,
    vertex_size = v_sizes,
    vertex_label_size = label_sizes,
    vertex_label_dist = 2,
    #~ vertex_color = colors,
    vertex_color = 90,
    vertex_label_color = colors,
    vertex_order_by = ("order","asc"),
    margin = (50,50,50,50),
    opacity = 1,
    palette = pal,
    background = None,              # This is only possible after my changes to igraph.__init__.py
    #~ edge_color = edgeColors,
    edge_color = 'red',
    edge_curved= False,
    edge_width = edgeWidths,
    edge_arrow_size = [x/20.0 for x in v_sizes]
    #~ vertex_label_color = "white",
    
)
g = h.copy()
g.es.delete(h.es)




# Plot graph without edges
p.add(g,
    #layout = "large",
    layout = mylayout,
   
    vertex_size = v_sizes,
    vertex_label_size = label_sizes,
    vertex_label_dist = 2,
    vertex_color = colors,
    vertex_label_color = colors,
    vertex_order_by = ("order","asc"),
    margin = (50,50,50,50),
    opacity = 1.0,
    palette = pal,
    background = None,              # This is only possible after my changes to igraph.__init__.py
    edge_color = edgeColors,
    edge_width = edgeWidths,
    
    #~ vertex_label_color = "white",
    
)
p.save()
#~ p.save("adjnoun-vertices-edges.pdf")


quit()


# Plot graph without edges
myplot(g,
    "adjnoun-vertices-edges.pdf",
    canvas = p,
    #layout = "large",
    layout = mylayout,
   
    vertex_size = sizes,
    vertex_label_size = sizes,
    vertex_label_dist = 2,
    vertex_color = colors,
    vertex_label_color = colors,
    vertex_order_by = ("order","asc"),
    margin = (50,50,50,50),
    opacity = 1.0,
    palette = pal,
    background = None,              # This is only possible after my changes to igraph.__init__.py
    edge_color = edgeColors,
    edge_width = edgeWidths,
    draw = True
    #~ vertex_label_color = "white",
    
)


quit()


































# Plot graph with edges
ig.plot(h,
    "adjnoun-vertices.pdf",
    #layout = "large",
    layout = mylayout,
    vertex_size = sizes,
    vertex_label_size = sizes,
    vertex_label_dist = 2,
    vertex_color = colors,
    vertex_label_color = colors,
    vertex_order_by = ("order","asc"),
    margin = (50,50,50,50),
    opacity = 0.4,
    palette = pal,
    background = None,              # This is only possible after my changes to igraph.__init__.py
    edge_color = edgeColors,
    edge_width = edgeWidths
    #~ vertex_label_color = "white",
    
)
h.es.delete(h.es)


# Plot graph without edges
ig.plot(h,
    "adjnoun-vertices-edges.pdf",
    #layout = "large",
    layout = mylayout,
    
    vertex_size = sizes,
    vertex_label_size = sizes,
    vertex_label_dist = 2,
    vertex_color = colors,
    vertex_label_color = colors,
    vertex_order_by = ("order","asc"),
    margin = (50,50,50,50),
    opacity = 1.0,
    palette = pal,
    background = None,              # This is only possible after my changes to igraph.__init__.py
    edge_color = edgeColors,
    edge_width = edgeWidths
    #~ vertex_label_color = "white",
    
)




# get adjacency matrix of graph as an array
M = array(h.get_adjacency().data)

# Plot the adjacency matrix
imshow(M, cmap = "gist_yarg", interpolation = "None")
#~ show()





