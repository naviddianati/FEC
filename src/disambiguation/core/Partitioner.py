# from collections import deque
#import networks as nt
import igraph as ig
import matplotlib.pyplot as plt
from disambiguation import config
import cPickle
import bisect



class Partitioner():
    '''
    Class for dividing a graph into edge partitions with
    minimal vertex overlap.
    '''
    def __init__(self, g, num_partitions = 10):
        self.g = g
        self.num_partitions = num_partitions
        self.n_edges_per_partition = 0
        self.list_edgelists = []
        
        self.dict_colors = {1: 'red',
                   2: 'blue',
                   3: 'green',
                   4: 'yellow',
                   5:'black',
                   6:'cyan'
                  }
        # Components of the graph
        self.components = None
        
        # dict component_id: size
        self.dict_component_sizes = {}

        self.mylayout = None
        
        
    def __get_color(self,group):
        if not group: return '#dddddd'
        try: 
            return self.dict_colors[group]
        except:
            return 'gray'
        
    def compute_layout(self):
#         self.mylayout = self.g.layout_fruchterman_reingold()
        self.mylayout = self.g.layout_graphopt()
#         self.mylayout = self.g.layout_drl()
    
    def backup_g(self):
        self.g_backup = cPickle.loads(cPickle.dumps(self.g))
        

    def tally_leftover_components(self):
        # A dictionary that maps a group id to
        # its size.
        dict_component_sizes = {}
        for group in self.components.membership:
            try:
                self.dict_component_sizes[group] += 1
            except:
                self.dict_component_sizes[group] = 1

        # A list of tuples (component_id, size), sorted
        self.list_component_sizes = self.dict_component_sizes.items()
        self.list_component_sizes.sort(key = lambda x:x[1], reverse = True)

        # A dict that maps vertex id to component id
        # dict_v_ids = {x:y for x,y in enumerate(components.membership)}

        # A dict that maps component id to list of vertex_ids
        self.dict_v_ids = {}
        for vid, component_id in enumerate(self.components.membership):
            try:
                self.dict_v_ids[component_id].append(vid)
            except:
                self.dict_v_ids[component_id] = [vid]
        
    
    def get_small_component_edgelists(self, skip_giant = False):
        '''
        loop through all components except for the giant component,
        and for each one yield the list of edges as (e.source, e.target)
        @param g: the original graph.
        @param list_component_sizes: A list of tuples (component_id, size),
        sorted in descending order.
        @param dict_v_ids: A dict that maps component id to list of vertex_ids
        @param skip_giant: whether to skip the giant component or not. We
        won't skip the giant component when it's small enough to fit into
        a partition and we don't need to partition it first.
        '''
        g = self.g
        list_component_sizes = self.list_component_sizes
        dict_v_ids = self.dict_v_ids
        
        start_index = 1 if skip_giant else 0
        for component_id, size in list_component_sizes[start_index:]:
        #     print component_id, size
            list_vids = dict_v_ids[component_id]
            edgelist = []

            if size > 2:
                for vid in list_vids: self.g.vs[vid]['original_index'] = self.g.vs[vid].index
                g_component = g.induced_subgraph(list_vids)
                
                for e in g_component.es:
#                     edgelist.append(tuple(sorted([g_component.vs[e.source]['original_index'], g_component.vs[e.target]['original_index']])))
                    edgelist.append(tuple(sorted([g_component.vs[e.source]['original_index'], g_component.vs[e.target]['original_index']])))
#                 print "added %d edges....." % len(edgelist)
            elif size == 2:
#                 print "added one edge..."
                edgelist = [tuple(sorted(list_vids))]
            else:
                pass
            yield edgelist


        
        
    
    
    def collect_leftovers(self):
        '''
        Put together the small components left over into
        partitions. This method should be called after the
        Giant component is broken up enough times so that 
        it's finally smaller than the desired partition size.
        Now we have a large number of small leftover components
        that should be clustered into the remaining partitions.
        '''
        # compute self.dict_v_ids and self.dict_component_sizes
        self.tally_leftover_components()
#         print "component sizes after giant partition:", self.list_component_sizes
        self.fill_in_the_blanks()
       


    
    def fill_in_the_blanks(self):
        '''
        Create a temp list of edgelists, and using
        L{get_small_component_edgelists}, fill them with the
        edgelists of the leftover small component edgelists.
        '''
        # Number of remaining edges in the graph after
        # the giant component is broken up into small pieces.
        num_remaining_partitions = self.num_partitions - len(self.list_edgelists)
        list_edgelists = [[] for i in range(num_remaining_partitions)]
        if not list_edgelists: return
        
        for edgelist in self.get_small_component_edgelists():
#             print 'len of leftover edgelists: ', [len(x) for x in list_edgelists]
            list_edgelists[0] += edgelist
            list_edgelists.sort(key = lambda item:len(item))
        
        self.list_edgelists += list_edgelists
        

    def partition(self):
        
        # A flag that shows which node has been chosen
        # as the seed for a partition.
        self.g.vs['seed'] = False

        # Find the connected components
        self.components = self.g.components()

        # find the giant component
        g_giant = self.components.giant()

        # Number of edges of the giant component
        n_edges_giant = g_giant.ecount()


        # Desired number of edges in each partition
        self.n_edges_per_partition = self.g.ecount() / self.num_partitions
        print "number of edges per partition: ", self.n_edges_per_partition

        # As long as the "size" of the giant component
        # is larger than the desired chunk size, extract a 
        # partition from it and break it up.
        counter = 0
        while n_edges_giant > self.n_edges_per_partition:
            print "iteration ", counter
            counter += 1

            # Get the edgelist of the partition extracted
            # from the giant component. And, alter the graph
            # by deleting those edges. Now, the graph has changede
            # and has a new giant component. The loop continues
            # until this giant component is small enough.

            # TODO: implement
            edgelist = self.extract_partition()
            self.list_edgelists.append(edgelist)

            # Find the connected components
            self.components = self.g.components()

#             print "Component sizes: ", sorted(self.components.sizes(), reverse = True)
            
            # find the giant component
            g_giant = self.components.giant()

            # Number of edges of the giant component
            n_edges_giant = g_giant.ecount()
        
        
        # Now, all the remaining components are smaller than
        # the desired partition size.
        self.collect_leftovers()
    

   

            
    
    

    def extract_partition(self):     
        gg = self.g
        
        # Actually a set, not a list
        edgelist = set()

        # id of the node to start the walk from
        start_id = self.get_start_id()

        # Buffer of node ids. Push from left, pop from right.
#         node_buffer_nonleaf = deque([start_id])
#         node_buffer_leaf = deque([])
        
        node_buffer = [start_id]

        current_group_id = 1

#         gg.vs['group'] = False
#         gg.vs[start_id]['group'] = current_group_id 


        # compute the degrees once and assign to node attribute.
        gg.vs['degree'] = gg.degree()

        # Explore the graph until
        while len(edgelist) < self.n_edges_per_partition :

            try:
                vid = node_buffer.pop()
            except:
                break

            v = gg.vs[vid]

            # Sort the list of neighbors by their degrees in 
            # descending order
            list_neighbors_all = sorted(v.neighbors(), key = lambda u : u.degree(), reverse = True)
            list_neighbors_all = [v for v in list_neighbors_all if v['degree'] > 0 ]

            for neighbor in list_neighbors_all:
#                 if neighbor.degree() == 1:
                if neighbor['degree'] > 1:
                    bisect.insort_right(node_buffer, neighbor.index)
                neighbor['degree'] -= 1
                
#                 neighbor['group'] = current_group_id
            set_edges_tmp = set([tuple(sorted([vid, neighbor.index])) for neighbor in list_neighbors_all])
            edgelist.update(set_edges_tmp)
        
            # Edges must be deleted only once for each partition.
            # Deleting them one at a time is incredibly slow.
        gg.delete_edges(edgelist)
            
            
        print "gg.ecount(): ", gg.ecount()
#         gg.delete_edges(edgelist)
#         list_singletons = [v.index for v in gg.vs if v.degree() == 0]
#         gg.delete_vertices(list_singletons)
        #gg.vs['color'] = [self.__get_color(v['group'])  for v in gg.vs]
        return edgelist

    
    
    def __get_highest_degree_in_giant(self):
        giant_component_id = sorted(list(enumerate(self.components.sizes())), key=lambda x:x[1],\
                                    reverse=True)[0][0]
        giant_component_vids =  [vid for vid, component_id in enumerate(self.components.membership) \
                                 if component_id == giant_component_id]
        giant_component_degrees = [(vid, self.g.vs[vid].degree()) for vid in giant_component_vids]
        giant_component_degrees.sort(key = lambda x:x[1], reverse = True)
        
        vid_max = giant_component_degrees[0][0]
        self.g.vs[vid_max]['seed'] = True
        return vid_max
        
        # Just return the first one
        return giant_component_vids[0]


    def get_start_id(self):
        '''
        Get the id of the higest-degree node in
        the giant component.
        '''
        return self.__get_highest_degree_in_giant()

    
    def print_stats(self):
        # Each element is the vids of the nodes relevant to
        # a partition
        list_list_nodes = [set([node for edge in edgelist for node in edge])   for edgelist in self.list_edgelists]
        set_all_nodes = {node for list_nodes in list_list_nodes for node in list_nodes}
        list_vcounts = [len(x) for x in list_list_nodes]
        list_ecounts = [len(x) for x in self.list_edgelists]

        print 'list of all partition node counts: ', list_vcounts
        print 'Total number of nodes in all partitions: ', sum(list_vcounts)
        print 'Total number of unique nodes in all partitions: ', len(set_all_nodes)
        
        print "list of all partition edge counts:", list_ecounts
        set_all_edges = set([edge for edgelist in self.list_edgelists for edge in edgelist])
        print "Total number of edges in all partitions: ", sum([len(x) for x in self.list_edgelists])
        print "Total number of unique edges in all partitions: ", len(set_all_edges)
        
    def plot(self, filename):
        try:
            g = self.g_backup
        except:
            print "In order to plot the graph, you must backup self.g before partitioning."
        list_edgelists = self.list_edgelists
        edgelist_group = {edge: i for i,tmp in enumerate(list_edgelists) for edge in tmp}
        edgelist = [edge for tmp in list_edgelists for edge in tmp]
        
        g.es['color'] = 'gray'
        for edge in edgelist:
            try:
                e = g.es.find(_between=((edge[0],), (edge[1],)))
                e['color'] = edgelist_group[edge]

            except Exception as error:
                print error, edge

        
#         g_tmp = ig.Graph.TupleList(edgelist)
#         edgecolors = [x[1] for x in edgelist_group]
#         g_tmp.es['color'] = edgecolors
        n_partitions = len(self.list_edgelists)
        ig.plot(g, filename,
            vertex_size = [14 if v['seed'] else 4 for v in self.g.vs],
            vertex_color = '#000000',
            edge_color = g.es['color'],
            edge_width = 1,
            palette = nt.mplPalette(n_partitions, 'hsv'),
#             vertex_label = [str(v.index) for v in g.vs],
            vertex_label_dist = 2,
            layout = self.mylayout
        )
        
    def plot_g(self, filename):
        '''
        Plot the original graph.
        '''
        g = self.g_backup
        self.compute_layout()
        ig.plot(g, filename,
            vertex_size = 4,
            vertex_color = '#000000',
            edge_color = 'gray',
            edge_width = 1,
#             palette = nt.mplPalette(n_partitions, 'hsv'),
            vertex_label = [str(v.index) for v in g.vs],
            vertex_label_dist = 2,
            layout = self.mylayout
        )
    
     
    def get_named_edgelist(self, partition_number):
        ''' Genereator'''
#         for edgelist in self.list_edgelists:
        edgelist = self.list_edgelists[partition_number]
        for edge in edgelist:
            vid1, vid2 = edge
            yield (self.g.vs[vid1]['name'], self.g.vs[vid2]['name'])