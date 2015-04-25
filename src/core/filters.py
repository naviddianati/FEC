"""
This module implements functions for filtering edges of a graph
based on the significance.
"""

from statsmodels.stats.proportion import binom_test
import numpy as np
from math import log




def pvalue(mode="undirected", **params):
    """
    Compute the p-value of a given edge according the appropriate null model.

    @param mode: can be "directed" or "undirected".

    Other parameters are different for the B{directed} and B{undirected} cases.
    See L{__pvalue_directed} and L{__pvalue_undirected} for detailed description of parameters.
    """

    if mode == "undirected":
        return __pvalue_undirected(**params)
    elif mode == "directed":
        return __pvalue_directed(**params)
    else:
        raise ValueError("mode must be either 'directed' or 'undirected'.")




def __pvalue_undirected(**params):
    """
    Compute the pvalue for the undirected edge null model.
    Use a standard binomial test from the statsmodels package

    @param w: weight of the undirected edge.
    @param ku: total incident weight (strength) of the first vertex.
    @param kv: total incident weight (strength) of the second vertex.
    @param q: total incident weight of all vertices divided by two. Similar to the total number of edges in the graph.
    """
    w = params.get("w")
    ku = params.get("ku")
    kv = params.get("kv")
    q = params.get("q")

    if not (w and ku and kv and q):
        raise ValueError


    p = ku * kv * 1.0 / q / q / 2.0
    return binom_test(count=w , nobs=q  , prop=p , alternative="larger")





def __pvalue_directed(**params):
    """
    Compute the pvalue for the directed edge null model.
    Use a standard binomial test from the statsmodels package

    @param w_uv: Weight of the directe edge.
    @param ku_out: Total outgoing weight of the source vertex.
    @param kv_in: Total incoming weight of the destination vertex.
    @param q: Total sum of all edge weights in the graph.
    """
    
    w_uv = params.get("w_uv")
    ku_out = params.get("ku_out")
    kv_in = params.get("kv_in")
    q = params.get("q")
    
    p = 1.0 * ku_out * kv_in / q / q / 1.0
    print "p = %f" % p
    return binom_test(count=w_uv , nobs=q  , prop=p , alternative="larger")



def prune(G, field='significance', percent=None, num_remove=None):
    """
    Remove all but the top x percent of the edges of the graph
    with respect to an edge attribute.

    @param G: an igraph Graph instance.
    @param field: the edge attribute to prune with respect to.
    @param percent: percentage of the edges with the highest field value to retain.
    @param num_remove: number of edges to remove. Used only if percent is None.
    """ 

    # # This is a little experiment where instead of "weight",
    # # I prune based on 'kk' which is k_u * k_v.
    # if field == 'weight': field = 'kk'
    # strengths = G.strength(weights = "weight")
    # for e in G.es:
    #    id0, id1 = e.source, e.target
    #    e['kk'] = - strengths[id0] * strengths[id1]

    if percent: 
        deathrow = []
        n = len(G.es)
        threshold_index = n - n * percent / 100
        threshold_value = sorted(G.es[field])[threshold_index]

        for e in G.es:
            if e[field] < threshold_value:
                deathrow.append(e.index)
        G.delete_edges(deathrow)
    elif num_remove:
        sorted_indices = np.argsort(G.es[field])
        G.delete_edges(sorted_indices[:num_remove])
    return G




def compute_significance(G):
    """
    Compute the edge significance for the edges of the
    given graph g in place. G.es['weight'] and G.vs['size']
    are expected to have been set already.

    TODO: implement the directed case as well.
    """
    ks = G.strength(weights='weight')
    total_degree = sum(ks)

    for e in G.es:
        i0, i1 = e.source, e.target
        v0, v1 = G.vs[i0], G.vs[i1]
        try:
            p = pvalue(w=e['weight'], ku=ks[i0], kv=ks[i1], q=total_degree / 2.0)
            e['significance'] = -log(p)
        except ValueError:
            # print e['weight'], ks[i0], ks[i1], total_degree, p
            e['significance'] = None
        
            # print "error computing significance", p
    
    max_sig = max(G.es['significance'])
    for e in G.es:
        if e['significance'] is None: e['significance'] = max_sig  



 


if __name__ == "__main__":
    pass






