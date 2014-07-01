'''
Created on Dec 6, 2013

@author: navid
'''
from igraph import color_name_to_rgba
import igraph
import os
import random
import string

from mypalettes import gnuplotPalette1, mplPalette
import numpy as np


class GraphStats():

    
    def __get_degree_sequence(self):
        '''compute the degree sequence of the graph'''
        degree_sequence = []
        for v in self.G.vs:
            degree_sequence.append(v.degree())
        return degree_sequence

    def __get_degree_distribution(self):
        '''compute the degree distribution of the graph'''
        degree_distribution = {}
        for degree in self.degree_sequence:
            if degree not in degree_distribution:
                degree_distribution[degree] = 1
            else:
                degree_distribution[degree] += 1
        return degree_distribution
                
        
    def save_all(self, path,test = False):
        id = 'py' if test else self.id 
        if not os.path.isdir(path):
            os.makedirs(path)
       
        '''Save the degree distribution'''
        f = open(path + id + '-degree_distribution.txt','w')
        for k in sorted(self.degree_distribution.keys()):
            f.write('%d %d\n' % (k, self.degree_distribution[k]))
        f.close()
        
        
    
    def id_generator(self, size=5, chars=string.ascii_uppercase + string.digits):
        return ''.join([random.choice(chars) for x in range(size)])          
    
    
    def __init__(self, G):
        self.id = self.id_generator()
        self.degree_sequence = []
        self.G = G
        self.N = len(G.vs)
        self.degree_sequence = self.__get_degree_sequence()
        self.degree_distribution = self.__get_degree_distribution()
    
    
        



def main():  
    # batch_id = 76  # OH
    batch_id = 76  # MA
    # batch_id = 71   # Ohio     
    data_path = os.path.expanduser('~/data/FEC/')
    G = igraph.Graph.Load(f=data_path + str(batch_id) + '-employer_graph.gml', format='gml')
    # G = igraph.Graph.Load(f=data_path + str(batch_id) + '-employer_graph_component-1.gml', format='gml')
    stats = GraphStats(G)
    stats.save_all('../results/stats/batch-'+str(batch_id)+"/",test=True)




if __name__ == '__main__':
    main()
