
# coding: utf-8

# In[1]:

import pandas as pd
import igraph as ig
from scipy.stats import binom
from statsmodels.stats.proportion import binom_test
from networks.visualization import *
from networks.pruning import *
from networks.complexity import *
from networks.examples import *
from math import log
import matplotlib.pyplot as plt
import matplotlib as  mpl
import numpy as np
from random import randint

from cPickle import dumps, loads

   

datapath = "../data/"



def compute_entropy(G, doplot = False):
    """
    Compute the entropy of random walks on the graph
    of a few small lengths, as a function of the 
    percentage of edges retained.
    """
        
    G_pickle = dumps(G)
    N = len(G.vs)
    pe = range(1,101,2)
    n_points = len(pe)
    pv1 = np.zeros([n_points,2])
    pv2 = np.zeros([n_points,2])

    field = 'significance'
    for i,percentage in enumerate(pe):
        print percentage
        G = loads(G_pickle)
        prune(G,field=field,percent = percentage)
        #clustering = G.transitivity_local_undirected(weights = field, mode="zero")
        giant = G.components().giant()
        entropy = random_walk_entropy(giant, 2)
        pv1[i,:] = [entropy[1], entropy[2]]

    field = 'weight'
    for i,percentage in enumerate(pe):
        print percentage
        G = loads(G_pickle)
        prune(G,field=field,percent = percentage)
        #clustering = G.transitivity_local_undirected(weights = field, mode="zero")
        giant = G.components().giant()
        entropy = random_walk_entropy(giant, 2)
        pv2[i,:] = [entropy[1], entropy[2]]


    print pv1
    print pv2   
    #plt.figure(figsize=(6,4))
    plt.plot([x/100. for x in pe],pv1[:,0],'o-',markersize=5,c="red",label=r'Significance $m=1$')
    plt.plot([x/100. for x in pe],pv1[:,1],'-',markersize=5,c="red",label=r'Significance $m=2$')
    p, = plt.plot([x/100. for x in pe],pv2[:,0],'o--',markersize=5,c="#000000",alpha=0.8,dash_capstyle="round",label=r'Weight $m=1$')
    p, = plt.plot([x/100. for x in pe],pv2[:,1],'--',markersize=5,c="#000000",alpha=0.8,dash_capstyle="round",label=r'Weight $m=2$')
    p.set_dashes([10,5])
    #plt.title("Average clustering coefficient for the giant component")
    #plt.legend(['Significance','Weight'],loc=4)
    plt.legend()
    #plt.xlabel("$|E_{f}|/|E|$",fontsize=20)
    plt.ylabel("$h_{f}$",fontsize=20)
    #plt.ylim([0,1.02])
    plt.tight_layout()
    
    if doplot:
        plt.savefig(datapath+"entropy.pdf")
        plt.show()








mpl.rcParams['font.family'] = "CMU Serif"
mpl.rcParams['lines.linewidth'] = 2



def graph_measures_random(preserve_mode = "degree", title = ""):
    G = get_airports_network()
    G_rand = get_randomized_weighted_network(G, preserve=preserve_mode , directed = False)
    compute_significance(G_rand)
    f = plt.figure()

    if title:
        plt.suptitle(title,fontsize=20)

    plt.subplot(2,2,1)
    compute_number_of_nodes(G_rand)
    #compute_total_weight(G)
    #plot_betweenness(G)
    plt.subplot(2,2,2)
    compute_clustering(G_rand)
    plt.subplot(2,2,3)
    compute_average_path_length(G)
    #compute_diameter(G_rand)
    plt.subplot(2,2,4)
    #compute_clique_number(G_rand)

    f.subplots_adjust(top=0.85)
    plt.savefig("../data/graph-measures-random-preserve-%s.pdf" % preserve_mode)
    plt.gcf().clear()


def graph_measures_airports():
    plt.rcParams['figure.figsize'] = 8,5
    G = get_airports_network()
    plt.subplot(2,2,1)
    compute_number_of_nodes(G)
    plt.subplot(2,2,2)
    compute_clustering(G)
    plt.subplot(2,2,3)
    
    compute_average_path_length(G)
    #compute_diameter(G)
    plt.subplot(2,2,4)
    compute_clique_number(G)
    plt.savefig("../data/graph-measures-airports.pdf")

    plt.rcParams['figure.figsize'] = 5,5
    compute_connectivity(G)


def graph_measures_airports_new(year, column, threshold):
    G = get_airports_network_new(year = year, column=column, volume_threshold=threshold, mode='undirected')
    #for e in G.es:
    #    e['weight'] = 1 
    plt.subplot(2,2,1)
    compute_number_of_nodes(G)
    #compute_total_weight(G)
    #plot_betweenness(G)
    plt.subplot(2,2,2)
    compute_clustering(G)
    plt.subplot(2,2,3)
    compute_diameter(G)
    plt.subplot(2,2,4)
    compute_clique_number(G)
    plt.savefig("../data/graph-measures-airports-new-%s-%s.pdf" % (column,year))


def normalize_names():
    G = get_airports_network()
    names = sorted(list(set(G.vs['label'])))

    f = open('../data/cities-raw.txt','w')
    for name in names:
        f.write("%s|%s\n" %(name, name))
    f.close()



def plot_stats(G, plot_title = ""):
    """
    Plot the histograms of edge significance and weight
    for graph G.
    """
    plt.subplot(2,1,1)
    plt.hist(G.es['significance'],bins = 50, color = 'black')
    plt.title('Significance', color = 'black')

    plt.subplot(2,1,2)
    plt.hist(G.es['weight'],bins = 50, color = 'black')
    plt.title('Weight', color = 'black')
    plt.suptitle(plot_title)
    





if __name__ == "__main__":
    year = '2012'
    plt.rcParams['figure.figsize'] = 8,5

    G = get_airports_network()
    compute_significance(G, order=2)
    #compute_entropy(G,True)
    #quit()

    #normalize_names()
    #graph_measures_random('degree', 'Randomized network with degrees \n preserved on average')
    #graph_measures_random('weight', 'Randomized network with edge \n weight sequence preserved')
    #quit()
    

    #graph_measures_airports()
    #quit()
    #graph_measures_airports_new(year, 'departures-performed', threshold = 50)
    #graph_measures_airports_new('passengers', threshold = 1000)
    #quit()

    #G = get_airports_network()
    #compute_connectivity(G,outfile="connectivity.pdf")
    #quit()
    

    #G.es['logweight'] = [np.log(w) for w in G.es['weight']]
    G.es['modweight'] = [np.sqrt(w) for w in G.es['weight']]
    #plt.plot(sorted(G.es['significance']))
    #plt.show()

    #plotted_attribute = 'departures-performed'
    plotted_attribute = 'passengers'
    #G = get_airports_network_new(year, plotted_attribute , volume_threshold = 36, mode = "undirected")

    ############################################################################
    # Compare the Alex network and its randomized version
    ############################################################################
    #G = get_airports_network()
    #plot_stats(G, 'Data')
    #plt.savefig('1.pdf')
    #print_summary(G)

    #plt.figure()
    #G_rand = get_randomized_weighted_network(G, preserve="degree", directed = False)
    #compute_significance(G_rand)
    #plot_stats(G_rand, 'Randomized')
    #plt.savefig('2.pdf')
    #G = G_rand
    #print_summary(G_rand)
    #quit()

    ############################################################################




    ############################################################################
    # Compare Alex's network and the FAA network
    ############################################################################
    #G1 = get_airports_network()
    #print G1.vcount(), G1.ecount()
    #plot_stats(G1)
    #plt.savefig('1.pdf')
    #plt.figure()
    #G2 = get_airports_network_new(year, plotted_attribute , volume_threshold = 320, mode = "undirected")
    #print G2.vcount(), G2.ecount()
    #plot_stats(G2)
    #plt.savefig('2.pdf')
    #quit()
    ############################################################################
    

    ############################################################################
    g = G
   
    ############################################################################





    strengths = G.strength(weights = "weight")
    for e in G.es:
        id0, id1 = e.source, e.target
        e['kk'] = - strengths[id0] * strengths[id1]
    
 
    print sorted(strengths)
    #print G.es['significance']

    #quit()


    G_pickle = dumps(G)

    G1 = loads(G_pickle)
    G2 = loads(G_pickle)

    settings1 = Settings(
        data_path=os.path.expanduser('~/data/GraphPruning/'),
        output_dim=(3200, 3200),
        num_components=(0, 1),
        verbose=False,
        v_max=120, v_min=10,
        e_max=50, e_min=2,
        l_max=70, l_min=10,
        e_percent= 20,
        l_percent=None,
        e_field_plot='weight',
        e_field_filter='weight',
        
        filter_components=False,
        background = "black", 
        output_label="%s-%s-plot-weight-filter-weight-20pct" % (year,plotted_attribute)
        )


    settings2 = Settings(
        data_path=os.path.expanduser('~/data/GraphPruning/'),
        output_dim=(3200, 3200),
        num_components=(0, 1),
        verbose=False,
        v_max=120, v_min=10,
        e_max=50, e_min=2,
        l_max=70, l_min=10,
        e_percent= 20,
        l_percent=20,
        e_field_plot='weight',
        e_field_filter='kk',
        
        filter_components=False,
        background = "white", 
        palette = "binary",
        palette_window = (0.5,1),
        layout_formatter = None, # layout_formatter, 
        output_label="%s-%s-new-plot-weight-filter-weight-20pct" % (year,plotted_attribute) 
        )


    # Pretty close to ideal
    settings3 = Settings(
        data_path=os.path.expanduser('~/data/GraphPruning/'),
        output_dim=(4200, 3200),
        num_components=(0, 1),
        verbose=False,
        v_max=120, v_min=15,
        e_max=20, e_min=4,
        l_max=90, l_min=25,
        e_percent= 15,
        l_percent=25,
        e_field_plot='weight',
        e_field_filter='significance',
        
        filter_components=True,
        background = "white", 
        palette = "gist_yarg",
        palette_window = (0.96,1),
        layout_formatter = layout_formatter, 
        output_label="%s-%s-new-plot-weight-filter-significance-20pct" % (year,plotted_attribute)
        )


    # Settled on this one for significance.
    settings4 = Settings(
        data_path=os.path.expanduser('~/data/GraphPruning/'),
        output_dim=(4200, 3200),
        num_components=(0, 1),
        verbose=False,
        v_max=120, v_min=15,
        e_max=20, e_min=2,
        l_max=90, l_min=25,
        e_percent= 15,
        l_percent=25,
        e_field_plot='modweight',
        e_field_filter='significance',
        e_color_mode='node', 
        filter_components=True,
        background = "white", 
        v_palette = "gist_yarg",
        v_palette_window = (0.8,1),
        layout_formatter = layout_formatter, 
        output_label="%s-%s-new-plot-weight-filter-significance-15pct" % (year,plotted_attribute)
        )




    # For weight
    settings5 = Settings(
        data_path=os.path.expanduser('~/data/GraphPruning/'),
        output_dim=(4200, 3200),
        num_components=(0, 1),
        verbose=False,
        v_max=120, v_min=15,
        e_max=20, e_min=2,
        l_max=90, l_min=25,
        e_percent= 15,
        l_percent=25,
        e_field_plot='modweight',
        e_field_filter='weight',
        
        filter_components=True,
        background = "white", 
        palette = "gist_yarg",
        palette_window = (0.8,1),
        layout_formatter = layout_formatter, 
        output_label="%s-%s-new-plot-weight-filter-weight-15pct" % (year,plotted_attribute)
        )



    # the new significance (strength) 
    settings6 = Settings(
        data_path=os.path.expanduser('~/data/GraphPruning/'),
        output_dim=(4200, 3200),
        num_components=(0, 1),
        verbose=False,
        v_max=120, v_min=15,
        e_max=20, e_min=2,
        l_max=90, l_min=25,
        e_percent= 10,
        l_percent=25,
        e_field_plot='modweight',
        e_field_filter='strength',
        
        filter_components=True,
        background = "white", 
        palette = "gist_yarg",
        palette_window = (0.8,1),
        layout_formatter = layout_formatter, 
        output_label="%s-%s-new-plot-weight-filter-strength-15pct" % (year,plotted_attribute)
        )

    #for settings in [settings1,settings2,settings3]:
    



    #plot_network(G,settings6)
    #quit()




    # render several times, significance
    for i in range(20):
        G = loads(G_pickle)
        #settings4.output_label="alt-%s-%s-new-plot-weight-filter-significance-2nd_order-15pct-%d" % (year,plotted_attribute, i)
        settings4.output_label="alt-%s-%s-new-plot-weight-filter-significance-2nd_order-15pct-long-run-%d" % (year,plotted_attribute, i)
        plot_network(G,settings4)
    #plot_network(G2,settings3)

    # Save graph to gml file
    #G.write_gml(settings.data_path + settings.output_label +".gml")

    # In[ ]:




    # In[ ]:



