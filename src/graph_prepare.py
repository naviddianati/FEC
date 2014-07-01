import json
import pprint
import igraph
import numpy as np
import re
import matplotlib.pyplot as plt

def bad_identifier(identifier,type='employer'):    
    if identifier == '': return True
    if type=='employer':
        regex = r'\bNA\b|N\.A|employ|self|N\/A|\
                |information request|retired|teacher\b|scientist\b|\
                |applicable|not employed|none|\
                |homemaker|requested|executive|educator\b|\
                |attorney\b|physician|real estate|\
                |student\b|unemployed|professor|refused|doctor|housewife|\
                |at home|president|best effort|consultant\b|\
                |email sent|letter sent|software engineer|CEO|founder|lawyer\b|\
                |instructor\b|chairman\b'
    elif type=='occupation':
        regex = r'unknown|requested|retired|none|retire|retited|ret\b|declined|N.A\b|refused|NA\b|employed|self'
    else:
        print 'invalid identifier type'
        quit()
        
    if re.search(regex, identifier, flags=re.IGNORECASE): 
        return True
    else: 
        return False
    
    
def show_histories_distribution(contributors_subgraphs):
    list_history_lengths = [len(g.vs) for g in contributors_subgraphs]
    plt.hist(list_history_lengths,bins=100,normed=True)
    plt.xscale('log')
    plt.yscale('log')
    plt.show()
    





    


debug = False

# affiliation='occupation'
affiliation='employer'





batch_id = 88  # NY
# batch_id = 89   # OH
batch_id = 90   # Delaware
batch_id = 91   # Missouri
batch_id = 83   # Alaska
# batch_id = 92  # Massachussetes
# batch_id = 93   # Nevada
# batch_id = 94   # Vermont
batch_id = 189  # NY



pp = pprint.PrettyPrinter(indent=4)
data_path = '/home/navid/data/FEC/'
# data_path = '../results/'
file_adjacency = open(data_path + str(batch_id) + '-adjacency.json')
file_nodes = open(data_path + str(batch_id) + '-list_of_nodes.json')
file_sample_affiliations = open(data_path + 'sample_'+affiliation+'s.json', 'w')

''' Gives a list of links where each link is a list:[source,target].
    Each node is an FEC transaction.'''

edgelist = json.load(file_adjacency)
dict_nodes = json.load(file_nodes)

# pp.pprint(adjacency)
G = igraph.Graph.TupleList(edges=edgelist)

# # (DIDN'T FIX THE PROBLEM) For some reason the vertex G.vas are automatically assigned and they are floats!
# # I'll convert them to strings, which is what igraph expects later
# for v in G.vs:
#     v['name']=str(int(v['name']))

clustering = G.components()

# The graph of the components: each component is contracted to one node
Gbar = clustering.cluster_graph()
print len(Gbar.vs)

dict_name_2_ind = {}
dict_ind_2_name = {}
affiliation_adjacency = {}
affiliation_score = {}
affiliation_names = []
affiliation_name_counter = 1
# Loop through the subgraphs, i.e., resolved individual identities.


contributors_subgraphs = clustering.subgraphs()


list_timelines = []
dict_timelines = {}
dict_affiliation_to_timelines = {}
counter_timelines = 0
list_sample_affiliation_groups = []


# show_histories_distribution(contributors_subgraphs); quit()


for counter, g in enumerate(contributors_subgraphs):
    if debug: 
        if counter > 10 : break
    list_affiliations = []
    
    # Loop through the nodes in each subgraph
    timeline = []
    dict_temp_affiliations = {}
    for v in g.vs:
        affiliation_index = 2 if affiliation == 'employer' else (6 if affiliation == 'occupation' else None)
        affiliation_identifier = dict_nodes[str(v['name'])]['aux'][affiliation_index]
        if bad_identifier(affiliation_identifier,type=affiliation): 
            if debug: print affiliation_identifier
            continue
        date = dict_nodes[str(v['name'])]['aux'][0]
        if affiliation_identifier not in dict_name_2_ind : 
            dict_name_2_ind[affiliation_identifier] = str(affiliation_name_counter)
            affiliation_name_counter += 1
            dict_ind_2_name[dict_name_2_ind[affiliation_identifier]] = affiliation_identifier

        affiliation_id = dict_name_2_ind[affiliation_identifier]
        
        ''' every unique affiliation occurring in this timeline should be associated with the timeline's id '''
        if affiliation_identifier not in dict_temp_affiliations:
            dict_temp_affiliations[affiliation_identifier] = 1
            try:
                dict_affiliation_to_timelines[affiliation_id].append(counter_timelines)
            except KeyError:
                dict_affiliation_to_timelines[affiliation_id] = []
                dict_affiliation_to_timelines[affiliation_id].append(counter_timelines)
            
        list_affiliations.append((affiliation_id, affiliation_identifier))
        
        timeline.append((date, affiliation_id))
    ''' Save this individual's timeline '''
    if timeline:
        timeline = sorted(timeline, key=lambda record: record[0])
#         list_timelines.append(sorted(timeline, key=lambda record: record[0]))
        dict_timelines[counter_timelines] = timeline
        counter_timelines += 1
            
    if debug: print "Number of unique affiliation strings: %d" % len(list_affiliations)
    if   len(list_affiliations) > 10:
        list_sample_affiliation_groups.append([x[1] for x in list_affiliations])
        for x in list_affiliations: print x[1]
        print '======================================'

    # Populate the affiliation adjacency matrix
    for ind1, name1 in list_affiliations:
        if ind1 not in affiliation_score: affiliation_score[ind1] = 0
        affiliation_score[ind1] += 1  # 0.1
#         for ind2, name2 in list_affiliations:
        for ind2,name2 in set( list_affiliations):
#             name2 = list_affiliations[ind2]
            if ind1 == ind2:
                continue
                
            link = (ind1, ind2)
            if link not in affiliation_adjacency:
                affiliation_adjacency[link] = 1
            else: 
                # This part can change depending on the likelihood function I ultimately decide to use
                affiliation_adjacency[link] += 1

''' The dictionary affiliation_adjacency assigns to each (source,target) tuple an integer weight.
    It is the sparse adjacency matrix of the inter-affiliation network.'''


''' The dictionary dict_timelines contains all the timelines with keys being integers.
    The dictionary dict_affiliation_to_timelines assigns to each affiliation id a list of id's of all timelines containing it'''
    
    
# # Plot the distribution of affiliation strings
#  plt.hist(affiliation_score.values(),bins=400)
#  plt.xscale('log')
#  plt.yscale('log')
#  plt.show()
#  quit()


# # print directed weights between affiliation string pairs
# a = affiliation_adjacency.keys()[:100]
# for i in range(100):
#     print affiliation_adjacency[(a[i][0],a[i][1])],affiliation_adjacency[(a[i][1],a[i][0])]
# quit()


file_sample_affiliations.write(json.dumps(list_sample_affiliation_groups))
file_sample_affiliations.close()


print 'Number of affiliation names: ', len(dict_name_2_ind), len(dict_ind_2_name)
# quit()
    

def get_likelihood(h):
    ''' Input: a list of 0s and 1s '''
    N = float(len(h))
    h = ''.join([str(x) for x in h])
    groups = re.findall(r'1+|0+', h)
    score = np.prod([len(g) / N for g in groups])
    
    score_top = 0.25
    score_allones = 1.0 / N * (N - 1) / N
    likelihood = score / score_top;
    return likelihood

def indicator(list_likelihoods):
    ''' returns a string of "+" characters whose length indicates the strength of the signal'''
    combined_likelihood = np.prod(list_likelihoods)
    combined_likelihood = 1e-6 if combined_likelihood <1e-6 else combined_likelihood 
    n = int(1. / combined_likelihood)
    n = -int(np.log(combined_likelihood)*5)+1
    if n > 40: n = 40
    s = ''.join(["+" for i in range(n)])
    return combined_likelihood, n, s


def link_pvalue(k1,k2,wab):
    pass


''' generate the list of prior parameters '''
dict_priors = {}
dict_likelihoods = {}
dict_posteriors = {}
for link in affiliation_adjacency:
    ''' Here, the value we call "prior" or "posterior" is 0 =< p =< 1
    where p is the probability that the two vertices are "similar" and 1-p
    the probability that they are "different". ''' 
    
    ## "clueless" prior
    # dict_priors[link] = 0.5
    
    ind0,ind1 = link[0],link[1]
    weight0,weight1=affiliation_adjacency[(ind0,ind1)],affiliation_adjacency[(ind1,ind0)]
    ratio0 = weight0/float(affiliation_score[ind0])
    ratio1 = weight1/float(affiliation_score[ind1])
    prior = np.max([ratio0,ratio1])
    print link, "%d/%d   %d/%d"%(weight0 , affiliation_score[link[0]],weight1 , affiliation_score[link[1]]) ,'    ','%0.2f %0.2f %0.2f' % (ratio0,ratio1,prior)
    print '          %s | %s' %(dict_ind_2_name[ind0],dict_ind_2_name[ind1])
    print "______________________________________________________________________________________________"
    dict_priors[link] = prior * 0.5
    dict_posteriors[link] = None



filename_list_of_dyads = data_path + str(batch_id) + '-list-dyads.txt'
f = open(filename_list_of_dyads, 'w')


list_tmp=[]

''' compute the posterior '''
for link in dict_priors:
    i0 = link[0]
    i1 = link[1]
    timelines0 = set(dict_affiliation_to_timelines[i0])
    timelines1 = set(dict_affiliation_to_timelines[i1])
    common_timelines = list(timelines0.intersection(timelines1))
    list_likelihoods = []
    list_timelines_collapsed = []
    for timeline_id in common_timelines:
        timeline = dict_timelines[timeline_id]
        timeline_collapsed = [0 if x[1] == i0 else 1 for x in timeline if x[1] == i0 or x[1] == i1]
#         print timeline
        list_timelines_collapsed.append(timeline_collapsed)
        list_likelihoods.append(get_likelihood(timeline_collapsed))

    ''' combined_likelihood gives the likelihood'''
    combined_likelihood, signal, signal_string = indicator(list_likelihoods)  
#     print signal
    if signal > -1000:
        s = dict_ind_2_name[i0] + ' | ' + dict_ind_2_name[i1] + "\n"
        for timeline_collapsed in list_timelines_collapsed:
            s += str(timeline_collapsed) + "\n" 
#         print (i0, i1)
#         s+=  'List of likelihoods: '+ str(list_likelihoods)+ '  \n                                              Signal: %s' %  signal_string +"\n"
        s += '                                                                Signal: %s' % signal_string + "\n"
        
        ''' Here, p is the probability that the two are different: \pi(0) '''
        p = 1 - dict_priors[link]
        ''' new_ratio is the ratio of posterior likelihoods of vertices being similar vs different. '''
        new_ratio = p / (1 - p) * combined_likelihood
        new_p = new_ratio / (1 + new_ratio)
        list_tmp.append(new_p)
        dict_posteriors[link] = 1-new_p
        dict_likelihoods[link] = 1/(1+combined_likelihood)
#         dict_posteriors[link] = combined_likelihood
        s += '%0.2f     %0.2f  --->   %0.2f      %0.2f\n'%(combined_likelihood,1-p,1-new_p,dict_likelihoods[link])
        s += '--------------------------------------------------------------------------------'
        print s
        f.write(s)
# quit()

        
f.close()
# plt.hist(list_tmp,bins=100)
# plt.show()

# prior_vs_posterior = np.array([[dict_priors[link],dict_posteriors[link]] for link in dict_posteriors])
# plt.plot(prior_vs_posterior[:,0],prior_vs_posterior[:,1],'o',alpha=0.1)
# plt.show()
# quit()


# quit()
    
 
    

  







            

# Generate the graph of linked affiliation names
tmp_adj = [(link[0], link[1], affiliation_adjacency[link], dict_likelihoods[link]) for link in affiliation_adjacency] 
G_affiliations = igraph.Graph.TupleList(edges=tmp_adj, edge_attrs=["weight", "confidence"])

for v in G_affiliations.vs:
    v['name'] = str(v['name'])

dd =  G_affiliations.degree_distribution()
# print np.sum(dd)
print dd.n, np.sum(affiliation_adjacency.values())

# Set the vertex labels
for v in G_affiliations.vs:
    v['label'] = dict_ind_2_name[v['name']]





# Set vertex sizes
for v in G_affiliations.vs:
#     v['size'] = round(np.log(affiliation_score[v['name']])+10)
    v['size'] = np.sqrt(affiliation_score[v['name']])




G_affiliations.save(f=data_path + str(batch_id) + '-'+affiliation +'_graph.gml', format='gml')

clustering = G_affiliations.components()

subgraphs = sorted(clustering.subgraphs(), key=lambda g:len(g.vs), reverse=True)
for g, i in zip(subgraphs[1:5], range(1, 5)):
    print len(g.vs)
    g.save(f=data_path + str(batch_id) + '-'+affiliation +'_graph_component-' + str(i) + '.gml', format='gml')


# save the giant component of the graph
G = clustering.giant()
G.save(f=data_path + str(batch_id) + '-'+affiliation +'_graph_giant_component.gml', format='gml')
# quit()




quit()




