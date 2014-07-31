'''
Created on Jun 30, 2014

@author: navid
'''
import igraph
import json
import os
import pprint
import re

import matplotlib.pyplot as plt
import numpy as np


class AffilliationAnalyzer(object):
    

    def __init__(self, batch_id=89, affiliation="employer"):
        self.debug = False
        self.data_path = os.path.expanduser('~/data/FEC/')

        self.batch_id = batch_id
        # affiliation='occupation'
        self.affiliation = affiliation
        self.pp = pprint.PrettyPrinter(indent=4)
        self.G = None
        self.dict_string_2_name = {}
        self.dict_name_2_string = {}
        self.affiliation_adjacency = {}
        self.affiliation_score = {}
#         self.affiliation_names = []
        self.affiliation_name_counter = 1
                
        self.list_timelines = []
        self.dict_timelines = {}
        self.dict_affiliation_to_timelines = {}
        self.counter_timelines = 0
        self.list_sample_affiliation_groups = []
        
        self.contributors_subgraphs = None
        self.load_settings()
        
#         batch_id = 88  # NY
#         # batch_id = 89   # OH
#         batch_id = 90  # Delaware
#         batch_id = 91  # Missouri
#         batch_id = 83  # Alaska
#         # batch_id = 92  # Massachussetes
#         # batch_id = 93   # Nevada
#         # batch_id = 94   # Vermont
#         batch_id = 189  # NY
    
    
    
    
    def show_histories_distribution (self):
        list_history_lengths = [len(g.vs) for g in self.contributors_subgraphs]
        plt.hist(list_history_lengths, bins=100, normed=True)
        plt.show()
        plt.xscale('log')
        plt.yscale('log')
        

    '''Load data.
    Here we load the data defining a graph of records. This graph should connect nodes that are
    determined to surely belong to the same individual. In other words, this graph should already
    be disambiguated with respect to individual identity.
    The purpose of this exercise is to find a subset of records with known identities, so that we
    can infer some statistics on their affiliations, etc. This subset of records doesn't have to be
    particularly large.
    '''
    def load_data(self):
        
        file_adjacency = open(self.data_path + str(self.batch_id) + '-adjacency.json')
        file_nodes = open(self.data_path + str(self.batch_id) + '-list_of_nodes.json')
        
        ''' Gives a list of links where each link is a list:[source,target].
            Each node is an FEC transaction.'''
        
        self.record_edge_list = json.load(file_adjacency)
        self.dict_record_nodes = json.load(file_nodes)
        self.G = igraph.Graph.TupleList(edges=self.record_edge_list)
    
    
    '''
    Computes the adjacency matrix between affiliation identifiers.
    The main product is self.affiliation_adjacency which is a dictionary {link:weight} where
    link is a tuple (ind1,ind2).
    The affiliation node indices are stored in two dictionaries self.dict_string_2_name  and
    self.dict_name_2_string.
    '''
    def extract(self):         
        clustering = self.G.components()

        # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
        self.contributors_subgraphs = clustering.subgraphs()
        
        # The graph of the components: each component is contracted to one node
        Gbar = clustering.cluster_graph()
        print len(Gbar.vs)
        
        # show_histories_distribution(contributors_subgraphs); quit()
        
        # Loop through the subgraphs, i.e., resolved individual identities.
        for counter, g in enumerate(self.contributors_subgraphs):
            if self.debug: 
                if counter > 10 : break
            list_affiliations = []
            
            # Loop through the nodes in each subgraph
            timeline = []
            dict_temp_affiliations = {}
            for v in g.vs:
                affiliation_index = self.settings['field_2_index'][self.affiliation.upper()]
#                 affiliation_index = 1 if self.affiliation == 'employer' else (6 if self.affiliation == 'occupation' else None)
                affiliation_identifier = self.dict_record_nodes[str(v['name'])]['data'][affiliation_index]
                if bad_identifier(affiliation_identifier, type=self.affiliation): 
                    if self.debug: print affiliation_identifier
                    continue   
                date_index = self.settings['field_2_index']['TRANSACTION_DT']
                date = self.dict_record_nodes[str(v['name'])]['data'][date_index]
                if affiliation_identifier not in self.dict_string_2_name : 
                    self.dict_string_2_name[affiliation_identifier] = str(self.affiliation_name_counter)
                    self.affiliation_name_counter += 1
                    self.dict_name_2_string[self.dict_string_2_name[affiliation_identifier]] = affiliation_identifier
        
                affiliation_id = self.dict_string_2_name[affiliation_identifier]
                
                ''' every unique affiliation occurring in this timeline should be associated with the timeline's id '''
                if affiliation_identifier not in dict_temp_affiliations:
                    dict_temp_affiliations[affiliation_identifier] = 1
                    try:
                        self.dict_affiliation_to_timelines[affiliation_id].append(self.counter_timelines)
                    except KeyError:
                        self.dict_affiliation_to_timelines[affiliation_id] = []
                        self.dict_affiliation_to_timelines[affiliation_id].append(self.counter_timelines)
                    
                list_affiliations.append((affiliation_id, affiliation_identifier))
                
                timeline.append((date, affiliation_id))
            ''' Save this individual's timeline '''
            if timeline:
                timeline = sorted(timeline, key=lambda record: record[0])
        #         list_timelines.append(sorted(timeline, key=lambda record: record[0]))
                self.dict_timelines[self.counter_timelines] = timeline
                self.counter_timelines += 1
                    
            if self.debug: print "Number of unique affiliation strings: %d" % len(list_affiliations)
            if   len(list_affiliations) > 10:
                self.list_sample_affiliation_groups.append([x[1] for x in list_affiliations])
                if self.debug:
                    for x in list_affiliations: print x[1]
                    print '======================================'
        
            # Populate the affiliation adjacency matrix
            for ind1, name1 in list_affiliations:
                if ind1 not in self.affiliation_score: self.affiliation_score[ind1] = 0
                self.affiliation_score[ind1] += 1  # 0.1
        #         for ind2, name2 in list_affiliations:
                for ind2, name2 in set(list_affiliations):
        #             name2 = list_affiliations[ind2]
                    if ind1 == ind2:
                        continue
                        
                    link = (ind1, ind2)
                    if link not in self.affiliation_adjacency:
                        self.affiliation_adjacency[link] = 1
                    else: 
                        # This part can change depending on the likelihood function I ultimately decide to use
                        self.affiliation_adjacency[link] += 1
        
        ''' The dictionary affiliation_adjacency assigns to each (source,target) tuple an integer weight.
            It is the sparse adjacency matrix of the inter-affiliation network.
        '''
        
        
        ''' The dictionary dict_timelines contains all the timelines with keys being integers.
            The dictionary dict_affiliation_to_timelines assigns to each affiliation id a list of id's of all timelines containing it
        '''
            
            


    def export_sample_affiliations(self):
        file_sample_affiliations = open(self.data_path + 'sample_' + self.affiliation + 's.json', 'w')
        file_sample_affiliations.write(json.dumps(self.list_sample_affiliation_groups))
        file_sample_affiliations.close()
            

    def load_settings(self):
        file_settings = open(self.data_path + str(self.batch_id) + '-settings.json')
        self.settings = json.load(file_settings)
        




    def save_data(self, label=""):
        if label == "":
            label = self.batch_id
        # Generate the graph of linked affiliation names
        tmp_adj = [(link[0], link[1], self.affiliation_adjacency[link], self.dict_likelihoods[link]) for link in self.affiliation_adjacency] 
        
        # Apparently, when a graph is generated from an edge list like this where each edge is an tuple (ind1,ind2), 
        # the resulting graph will assign ind1 and ind2 to the 'name' attribute of the generated vertices. Their index on the
        # other hand, is apparently generated randomly
        G_affiliations = igraph.Graph.TupleList(edges=tmp_adj, edge_attrs=["weight", "confidence"])
        
        for v in G_affiliations.vs:
            v['name'] = str(v['name'])
        
        dd = G_affiliations.degree_distribution()
        # print np.sum(dd)
        print dd.n, np.sum(self.affiliation_adjacency.values())
        
        # Set the vertex labels
        for v in G_affiliations.vs:
            v['label'] = self.dict_name_2_string[v['name']].encode('utf-8')
        
        # Set vertex sizes
        for v in G_affiliations.vs:
        #     v['size'] = round(np.log(affiliation_score[v['name']])+10)
            v['size'] = np.sqrt(self.affiliation_score[v['name']])
        
        G_affiliations.save(f=self.data_path + label + '-' + self.affiliation + '_graph.gml', format='gml')
        
        clustering = G_affiliations.components()
        
        subgraphs = sorted(clustering.subgraphs(), key=lambda g:len(g.vs), reverse=True)
        for g, i in zip(subgraphs[1:5], range(1, 5)):
            print len(g.vs)
            g.save(f=self.data_path + label + '-' + self.affiliation + '_graph_component-' + str(i) + '.gml', format='gml')
        
        # save the giant component of the graph
        G = clustering.giant()
        G.save(f=self.data_path + label + '-' + self.affiliation + '_graph_giant_component.gml', format='gml')
        
        # save affiliation identifier statistics
        
        f = open(self.data_path + label + '-' + self.affiliation + "-metadata.json", "w") 
        
        ''' affiliation_score is a dict {name:frequency} where '''
        
        dict_data = {
                     "affiliation_score" :self.affiliation_score, 
                      "batch_id": self.batch_id}
        f.write(json.dumps(dict_data))
        f.close()





    '''
    This function
    '''

    def compute_affiliation_links(self):
     
        ''' generate the list of prior parameters '''
        self.dict_priors = {}
        self.dict_likelihoods = {}
        self.dict_posteriors = {}
        for link in self.affiliation_adjacency:
            ''' Here, the value we call "prior" or "posterior" is 0 =< p =< 1
            where p is the probability that the two vertices are "similar" and 1-p
            the probability that they are "different". ''' 
            
            # # "clueless" prior
            # dict_priors[link] = 0.5
            
            ind0, ind1 = link[0], link[1]
            weight0, weight1 = self.affiliation_adjacency[(ind0, ind1)], self.affiliation_adjacency[(ind1, ind0)]
            ratio0 = weight0 / float(self.affiliation_score[ind0])
            ratio1 = weight1 / float(self.affiliation_score[ind1])
           
            # set the prior
            prior = np.max([ratio0, ratio1])
            
            self.dict_priors[link] = prior * 0.5
            self.dict_posteriors[link] = None

            continue
            if self.debug:            
                print link, "%d/%d   %d/%d" % (weight0 , self.affiliation_score[link[0]], weight1 , self.affiliation_score[link[1]]) , '    ', '%0.2f %0.2f %0.2f' % (ratio0, ratio1, prior)
                print '          %s | %s' % (self.dict_name_2_string[ind0], self.dict_name_2_string[ind1])
                print "______________________________________________________________________________________________"
        
        
        
        filename_list_of_dyads = self.data_path + str(self.batch_id) + '-list-dyads.txt'
        f = open(filename_list_of_dyads, 'w')
        
        
        list_tmp = []
        
        ''' compute the posterior '''
        for link in self.dict_priors:
            i0 = link[0]
            i1 = link[1]
            timelines0 = set(self.dict_affiliation_to_timelines[i0])
            timelines1 = set(self.dict_affiliation_to_timelines[i1])
            common_timelines = list(timelines0.intersection(timelines1))
            list_likelihoods = []
            list_timelines_collapsed = []
            for timeline_id in common_timelines:
                timeline = self.dict_timelines[timeline_id]
                timeline_collapsed = [0 if x[1] == i0 else 1 for x in timeline if x[1] == i0 or x[1] == i1]
        #         print timeline
                list_timelines_collapsed.append(timeline_collapsed)
                list_likelihoods.append(get_likelihood(timeline_collapsed))
        
            ''' combined_likelihood gives the likelihood'''
            combined_likelihood, signal, signal_string = indicator(list_likelihoods)  
        #     print signal
            if signal > -1000:
                s = self.dict_name_2_string[i0] + ' | ' + self.dict_name_2_string[i1] + "\n"
                for timeline_collapsed in list_timelines_collapsed:
                    s += str(timeline_collapsed) + "\n" 
        #         print (i0, i1)
        #         s+=  'List of likelihoods: '+ str(list_likelihoods)+ '  \n                                              Signal: %s' %  signal_string +"\n"
                s += '                                                                Signal: %s' % signal_string + "\n"
                
                ''' Here, p is the probability that the two are different: \pi(0) '''
                p = 1 - self.dict_priors[link]
                ''' new_ratio is the ratio of posterior likelihoods of vertices being similar vs different. '''
                new_ratio = p / (1 - p) * combined_likelihood
                new_p = new_ratio / (1 + new_ratio)
                list_tmp.append(new_p)
                self.dict_posteriors[link] = 1 - new_p
                self.dict_likelihoods[link] = 1 / (1 + combined_likelihood)
        #         dict_posteriors[link] = combined_likelihood
                s += '%0.2f     %0.2f  --->   %0.2f      %0.2f\n' % (combined_likelihood, 1 - p, 1 - new_p, self.dict_likelihoods[link])
                s += '--------------------------------------------------------------------------------'
                # print s
                f.write(s.encode('ascii','ignore'))
        f.close()
    



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
    combined_likelihood = 1e-6 if combined_likelihood < 1e-6 else combined_likelihood 
    n = int(1. / combined_likelihood)
    n = -int(np.log(combined_likelihood) * 5) + 1
    if n > 40: n = 40
    s = ''.join(["+" for i in range(n)])
    return combined_likelihood, n, s


def link_pvalue(k1, k2, wab):
    pass

        
def bad_identifier(identifier, type='employer'):    
    if identifier == '': return True
    if type == 'employer':
        regex = r'\bNA\b|N\.A|employ|self|N\/A|\
                |information request|retired|teacher\b|scientist\b|\
                |applicable|not employed|none|\
                |homemaker|requested|executive|educator\b|\
                |attorney\b|physician|real estate|\
                |student\b|unemployed|professor|refused|doctor|housewife|\
                |at home|president|best effort|consultant\b|\
                |email sent|letter sent|software engineer|CEO|founder|lawyer\b|\
                |instructor\b|chairman\b'
    elif type == 'occupation':
        regex = r'unknown|requested|retired|none|retire|retited|ret\b|declined|N.A\b|refused|NA\b|employed|self'
    else:
        print 'invalid identifier type'
        quit()
#     print identifier
    if re.search(regex, identifier, flags=re.IGNORECASE): 
        return True
    else: 
        return False
    
    
    
    
    
def main():
    batch_id = 1027
    '''analyst = AffilliationAnalyzer(batch_id=batch_id, affiliation="occupation")
    state = analyst.settings["param_state"]
    analyst.load_data()
    analyst.extract()
    analyst.compute_affiliation_links()
    analyst.save_data(label=state)
    '''
    analyst = AffilliationAnalyzer(batch_id=batch_id, affiliation="employer")
    state = analyst.settings["param_state"]
    analyst.load_data()
    analyst.extract()
    analyst.compute_affiliation_links()
    analyst.save_data(label=state)
    
    
def loadAffiliationNetwork(label, data_path, affiliation):
    G = igraph.Graph.Read_GML(f=data_path + label + '-' + affiliation + '_graph.gml')
    return G
    

if __name__ == "__main__":
    main()
    quit()
    
    