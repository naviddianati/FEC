'''
This module implements classes used to generate the affiliation (employer/occupation)
similarity graphs.
@todo: documentation of the classes is VERY incomplete.
'''


import igraph
import json
from math import log
import os
import pprint
import re

import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import binom
from Database import FecRetriever
import filters
import utils

def get_committees():
    ''' NOT COMPLETE'''

    retriever = FecRetriever(table_name='committee_master',
                      query_fields=['id', 'CMTE_ID', 'CAND_ID', 'CMTE_PTY_AFFILIATION'],
                      limit=(0, 100000000),
                      list_order_by='',
                      where_clause="")
    retriever.retrieve()

    list_of_records = retriever.getRecords()
    return list_of_records








class AffiliationAnalyzer(object):


    def __init__(self, state="", batch_id=None, affiliation="employer"):
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

        # dict {affiliation_id: score}
        self.affiliation_score = {}

        # dict {affiliation_id: party makeup} where party makeup is a
        # list such as [100,400,0] where the elements are the dollar
        # amounts given to the Democratic party, Republican party and Others.
        # This list will be jasonified before being exported to the .gml
        # graph file.
        self.affiliation_party_amount = {}


#         self.affiliation_names = []
        self.affiliation_name_counter = 1

        self.list_timelines = []
        self.dict_timelines = {}
        self.dict_affiliation_to_timelines = {}
        self.counter_timelines = 0
        self.list_sample_affiliation_groups = []

        self.contributors_subgraphs = None
        self.load_settings(file_label=state + "-" + "affiliations-")



        # Retrieve party affiliation data
        try:
            # dict of committe. The keys are CMTE_IDs and the values are
            # party codes CMTE_PTY_AFFILIATION'
            self.dict_committees = {}
            self.compile_committees()
        except:
            self.dict_committees = None


        # The graph of affiliation similarities. An igraph.Graph instance.
        # A weighted undirected simple graph. Nodes are affiliations, edges indicate
        # co-occurrence. The weight of the linke between two affiliations
        # counts the number of times one followed the other in a timeline.
        self.G_affiliations = None



    def compile_committees(self):
        """
        using get_committees(), create a  dict of committees

        """
        list_committees = get_committees()
        for r in list_committees:
            self.dict_committees[r['CMTE_ID']] = r['CMTE_PTY_AFFILIATION']




    def get_party(self, CMTE_ID):
        """
        Return the party affiliation of the committee id.
        0 for Democratic
        1 for Republican
        2 for Other.
        """

        # If committee info could not be retrieved, return None
        if self.dict_committees is None:
            return None

        try:
            party_str = self.dict_committees[CMTE_ID]
            if party_str == "DEM":
                party_id = 0
            elif party_str == "REP":
                party_id = 1
            else:
                party_id = 2


        except KeyError:
            party_id = None
        return party_id


    def show_histories_distribution (self):
        list_history_lengths = [len(g.vs) for g in self.contributors_subgraphs]
        plt.hist(list_history_lengths, bins=100, normed=True)
        plt.show()
        plt.xscale('log')
        plt.yscale('log')


    def load_data(self):
        '''Load data.
        Here we load the data defining a graph of records. This graph should connect nodes that are
        determined to surely belong to the same individual. In other words, this graph should already
        be disambiguated with respect to individual identity.
        The purpose of this exercise is to find a subset of records with known identities, so that we
        can infer some statistics on their affiliations, etc. This subset of records doesn't have to be
        particularly large.
        '''
        file_label = self.settings['state'] + "-" + "affiliations-"
        file_adjacency = open(self.data_path + file_label + str(self.batch_id) + '-adjacency.json')
        file_nodes = open(self.data_path + file_label + str(self.batch_id) + '-list_of_nodes.json')

        ''' Gives a list of links where each link is a list:[source,target].
            Each node is an FEC transaction.'''

        self.record_edge_list = json.load(file_adjacency)
        self.dict_record_nodes = json.load(file_nodes)
        self.G = igraph.Graph.TupleList(edges=self.record_edge_list)


    def extract(self):
        '''
        Computes the adjacency matrix between affiliation identifiers.
        The main product is self.affiliation_adjacency which is a dictionary {link:weight} where
        link is a tuple (ind1,ind2).
        The affiliation node indices are stored in two dictionaries self.dict_string_2_name  and
        self.dict_name_2_string.
        '''
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
                if utils.bad_identifier(affiliation_identifier, type=self.affiliation):
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


            # Iterate through the set of *unique* affiliations in this subgraph
            for ind1, name1 in set(list_affiliations):
                for ind2, name2 in set(list_affiliations):
                    if ind1 == ind2:
                        continue
                    link = (ind1, ind2)
                    if link not in self.affiliation_adjacency:
                        # for the directed edge (ind1,ind2), set the weight to be the number of name1 strings that appeared in a timeline that also includes name2
                        self.affiliation_adjacency[link] = list_affiliations.count((ind1, name1))
                    else:
                        self.affiliation_adjacency[link] += list_affiliations.count((ind1, name1))





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


    def load_settings(self, file_label=""):
        file_settings = open(self.data_path + file_label + str(self.batch_id) + '-settings.json')
        self.settings = json.load(file_settings)





    def save_data(self, label=""):
        if label == "":
            label = self.batch_id




        for v in self.G_affiliations.vs:
            v['name'] = str(v['name'])

        dd = self.G_affiliations.degree_distribution()
        # print np.sum(dd)
        print dd.n, np.sum(self.affiliation_adjacency.values())

        # Set the vertex labels
        for v in self.G_affiliations.vs:
            v['label'] = self.dict_name_2_string[v['name']].encode('utf-8')

        # Set vertex sizes
        for v in self.G_affiliations.vs:
            v['size'] = np.sqrt(self.affiliation_score[v['name']])
            v['party'] = json.dumps(self.affiliation_party_amount[v['name']])

        self.G_affiliations.save(f=self.data_path + label + '-' + self.affiliation + '_graph.gml', format='gml')

        clustering = self.G_affiliations.components()

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
        print "Saved affiliation graphs to file..."






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

            # This ratio now tells us what percentage of the occurrences of affiliation1 were in a timeline that also included affiliation2
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
            combined_likelihood = get_combined_likelihood(list_likelihoods)
            print combined_likelihood
        #     print signal
            if combined_likelihood > -1000:
                s = self.dict_name_2_string[i0] + ' | ' + self.dict_name_2_string[i1] + "\n"
                for timeline_collapsed in list_timelines_collapsed:
                    s += str(timeline_collapsed) + "\n"
        #         print (i0, i1)
        #         s+=  'List of likelihoods: '+ str(list_likelihoods)+ '  \n                                              Signal: %s' %  signal_string +"\n"

                ''' Here, p is the probability that the two are different: \pi(0) '''
                p = 1 - self.dict_priors[link]
                ''' new_ratio is the ratio of posterior likelihoods of vertices being similar vs different. '''
                new_ratio = p / (1 - p) * combined_likelihood
                new_p = new_ratio / (1 + new_ratio)
                list_tmp.append(new_p)
                self.dict_posteriors[link] = 1 - new_p
                self.dict_likelihoods[link] = 1 / (1 + combined_likelihood)
        #         dict_posteriors[link] = combined_likelihood

                signal_string = get_posterior_indicator(self.dict_posteriors[link])
                s += '                                                                Signal: %s' % signal_string + "\n"
#                 s += '%0.2f     %0.2f  --->   %0.2f      %0.2f\n' % (combined_likelihood, 1 - p, 1 - new_p, self.dict_likelihoods[link])
                s += '%0.2f     %0.2f  --->   %0.2f      %0.2f\n' % (combined_likelihood, 1 - p, 1 - new_p, self.dict_posteriors[link])
                s += '--------------------------------------------------------------------------------'
                # print s
                f.write(s.encode('ascii', 'ignore'))
        f.close()




def _likelihood_1(h):
    ''' The likelihood based on sizes of 000 or 111 chunks'''
    ''' Input: a list of 0s and 1s '''
    N = float(len(h))
    h = ''.join([str(x) for x in h])
    groups = re.findall(r'1+|0+', h)
    score = np.prod([len(g) / N for g in groups])

    score_top = 0.25
    score_allones = 1.0 / N * (N - 1) / N
    likelihood = score / score_top;
    return likelihood


def _likelihood_2(h):
    ''' TODO: The likelihood based on the number of switches'''
    ''' Input: a list of 0s and 1s '''
    N = float(len(h))

    h = ''.join([str(x) for x in h])
    groups = re.findall(r'1+|0+', h)
    score = np.prod([len(g) / N for g in groups])

    score_top = 0.25
    score_allones = 1.0 / N * (N - 1) / N
    likelihood = score / score_top;
    return likelihood


def get_likelihood(h):
#     return _likelihood_1(h)
    return _likelihood_2(h)

def get_combined_likelihood(list_likelihoods):
    combined_likelihood = np.prod(list_likelihoods)
    combined_likelihood = 1e-6 if combined_likelihood < 1e-6 else combined_likelihood
    return combined_likelihood


def get_posterior_indicator(posterior):
    ''' returns a string of "+" characters whose length indicates the strength of the signal'''
    n = int(posterior * 40)
    s = ''.join(["+" for i in range(n)])
    return  s


def link_pvalue(k1, k2, wab):
    pass




























class AffiliationAnalyzerDirected(AffiliationAnalyzer):



    def compute_affiliation_links(self):
        '''Overrides the original'''
        '''
        TODO: This is still identical to the undirected case. Must be updated.
        '''

        raise('ERROR: Directed version not implemented yet')

        # Generate the graph of linked affiliation names
        tmp_adj = [(link[0], link[1], self.affiliation_adjacency[link]) for link in self.affiliation_adjacency if link[0] != link[1]]
        print "number of links to save ", len(tmp_adj)

        # Apparently, when a graph is generated from an edge list like this where each edge is an tuple (ind1,ind2),
        # the resulting graph will assign ind1 and ind2 to the 'name' attribute of the generated vertices. Their index on the
        # other hand, is apparently generated randomly
        self.G_affiliations = igraph.Graph.TupleList(edges=tmp_adj, edge_attrs=["weight"])

        # simplify the graph. Remove self-edges, combine multi-edges
        # by summing their weights.
        self.G_affiliations.simplify(combine_edges=sum)

        # Compute edge significances. Adds a 'significance' attribute to the edges.
        filters.compute_significance(self.G_affiliations)


    def extract(self):
        '''Override
        Computes the adjacency matrix between affiliation identifiers.
        The main product is self.affiliation_adjacency which is a dictionary {link:weight} where
        link is a tuple (ind1,ind2).
        The affiliation node indices are stored in two dictionaries self.dict_string_2_name  and
        self.dict_name_2_string.
        '''
        clustering = self.G.components()

        # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
        self.contributors_subgraphs = clustering.subgraphs()

        # The graph of the components: each component is contracted to one node
        Gbar = clustering.cluster_graph()
        print "number of subgraphs:", len(Gbar.vs)

        # show_histories_distribution(contributors_subgraphs); quit()

        # Loop through the subgraphs, i.e., resolved individual identities.
        for counter, g in enumerate(self.contributors_subgraphs):
            if self.debug:
                pass
#                 if counter > 10 : break


            list_affiliations = []

            # Loop through the nodes in each subgraph
            timeline = []
            dict_temp_affiliations = {}
            for counter_v, v in enumerate(g.vs):
                affiliation_index = self.settings['field_2_index'][self.affiliation.upper()]
#                 affiliation_index = 1 if self.affiliation == 'employer' else (6 if self.affiliation == 'occupation' else None)
                affiliation_identifier = self.dict_record_nodes[str(v['name'])]['data'][affiliation_index]
                if utils.bad_identifier(affiliation_identifier, type=self.affiliation):
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
            else:
                continue

            # Do some verbose logging
            if self.debug: print "Number of unique affiliation strings: %d" % len(list_affiliations)
            if   len(list_affiliations) > 10:
                self.list_sample_affiliation_groups.append([x[1] for x in list_affiliations])
                if self.debug:
                    for x in list_affiliations: print x[1]
                    print '======================================'




            # With the new definition of adjacency, I need the time-ordered timeline for computin edge weights.
            # So, I must use the sorted timeline instead of list_affiliations
            for i in range(len(timeline) - 1):

                # ids of affiliations located in timeline[i] and timeline[i+1]
                id0, id1 = timeline[i][1], timeline[i + 1][1]
                link = (id0, id1)
                try:
                    self.affiliation_score[id0] += 1
                except KeyError:
                    self.affiliation_score[id0] = 1

                try:
                    self.affiliation_adjacency[link] += 1
                except KeyError:
                    self.affiliation_adjacency[link] = 1

                # Self-edge
                link = (id0, id0)
                try:
                    self.affiliation_adjacency[link] += 1
                except KeyError:
                    self.affiliation_adjacency[link] = 1





            # For the last item, create a self-edge
            id_last = timeline[-1][1]
            link = (id_last, id_last)
            try:
                self.affiliation_score[id_last] += 1
            except KeyError:
                self.affiliation_score[id_last] = 1
            try:
                self.affiliation_adjacency[link] += 1
            except KeyError:
                self.affiliation_adjacency[link] = 1

            # Self-edge
            link = (id_last, id_last)
            try:
                self.affiliation_adjacency[link] += 1
            except KeyError:
                self.affiliation_adjacency[link] = 1


        ''' The dictionary affiliation_adjacency assigns to each (source,target) tuple an integer weight.
            It is the sparse adjacency matrix of the inter-affiliation network.
        '''


        ''' The dictionary dict_timelines contains all the timelines with keys being integers.
            The dictionary dict_affiliation_to_timelines assigns to each affiliation id a list of id's of all timelines containing it
        '''
















class AffiliationAnalyzerUndirected(AffiliationAnalyzer):


    def compute_affiliation_links(self):
        '''
        Compute self.dict_likelihoods using self.affiliation_adjacency.
        The former assigns a likelihood to each edge, i.e, each (id0,id1) tuple.
        Note that since self.
        '''

        # Generate the graph of linked affiliation names
        tmp_adj = [(link[0], link[1], self.affiliation_adjacency[link]) for link in self.affiliation_adjacency if link[0] != link[1]]
        print "number of links to save ", len(tmp_adj)

        # Apparently, when a graph is generated from an edge list like this where each edge is an tuple (ind1,ind2),
        # the resulting graph will assign ind1 and ind2 to the 'name' attribute of the generated vertices. Their index on the
        # other hand, is apparently generated randomly
        self.G_affiliations = igraph.Graph.TupleList(edges=tmp_adj, edge_attrs=["weight"])

        # simplify the graph. Remove self-edges, combine multi-edges
        # by summing their weights.
        self.G_affiliations.simplify(combine_edges=sum)

        # Compute edge significances. Adds a 'significance' attribute to the edges.
        filters.compute_significance(self.G_affiliations)




    def extract(self):
        '''Override
        Computes the adjacency matrix between affiliation identifiers.
        The main product is self.affiliation_adjacency which is a dictionary {link:weight} where
        link is a tuple (ind1,ind2).
        The affiliation node indices are stored in two dictionaries self.dict_string_2_name  and
        self.dict_name_2_string.
        '''
        clustering = self.G.components()

        # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
        self.contributors_subgraphs = clustering.subgraphs()

        # The graph of the components: each component is contracted to one node
        Gbar = clustering.cluster_graph()
        print "number of subgraphs:", len(Gbar.vs)

        # show_histories_distribution(contributors_subgraphs); quit()

        # Loop through the subgraphs, i.e., resolved individual identities.
        for counter, g in enumerate(self.contributors_subgraphs):
            if self.debug:
                pass
#                 if counter > 10 : break


            list_affiliations = []

            # Loop through the nodes in each subgraph
            timeline = []
            dict_temp_affiliations = {}
            for counter_v, v in enumerate(g.vs):
                record_data = self.dict_record_nodes[str(v['name'])]['data']
                affiliation_index = self.settings['field_2_index'][self.affiliation.upper()]
                affiliation_identifier = record_data[affiliation_index]


                amount_index = self.settings['field_2_index']['TRANSACTION_AMT']
                amount = record_data[amount_index]

                committee_index = self.settings['field_2_index']['CMTE_ID']
                committee_id = record_data[committee_index]

                date_index = self.settings['field_2_index']['TRANSACTION_DT']
                date = record_data[date_index]

                if utils.bad_identifier(affiliation_identifier, type=self.affiliation):
                    if self.debug: print affiliation_identifier
                    continue

                if affiliation_identifier not in self.dict_string_2_name :
                    self.dict_string_2_name[affiliation_identifier] = str(self.affiliation_name_counter)
                    self.affiliation_name_counter += 1
                    self.dict_name_2_string[self.dict_string_2_name[affiliation_identifier]] = affiliation_identifier

                affiliation_id = self.dict_string_2_name[affiliation_identifier]


                # For each record we have to update the affiliation party statistics
                # namely which party the donation was made to.

                # Will be 0 for "D", 1 for "R", 2 for "Other"
                party_id = self.get_party(committee_id)

                if affiliation_id not in self.affiliation_party_amount:
                    self.affiliation_party_amount[affiliation_id] = [0, 0, 0]
                if party_id is not None:
                    self.affiliation_party_amount[affiliation_id][party_id] += amount





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
            else:
                continue

            # Do some verbose logging
            if self.debug: print "Number of unique affiliation strings: %d" % len(list_affiliations)
            if   len(list_affiliations) > 10:
                self.list_sample_affiliation_groups.append([x[1] for x in list_affiliations])
                if self.debug:
                    for x in list_affiliations: print x[1]
                    print '======================================'




            # With the new definition of adjacency, I need the time-ordered timeline for computin edge weights.
            # So, I must use the sorted timeline instead of list_affiliations
            for i in range(len(timeline) - 1):

                # ids of affiliations located in timeline[i] and timeline[i+1]
                id0, id1 = timeline[i][1], timeline[i + 1][1]
                try:
                    self.affiliation_score[id0] += 1
                except KeyError:
                    self.affiliation_score[id0] = 1


                for link in[(id0, id1), (id1, id0)]:
                    try:
                        self.affiliation_adjacency[link] += 1
                    except KeyError:
                        self.affiliation_adjacency[link] = 1

                # Self-edge (WHY?!?!?!)
                link = (id0, id0)
                try:
                    self.affiliation_adjacency[link] += 0
                except KeyError:
                    self.affiliation_adjacency[link] = 1





            # For the last item, create a self-edge
            id_last = timeline[-1][1]
            link = (id_last, id_last)
            try:
                self.affiliation_score[id_last] += 1
            except KeyError:
                self.affiliation_score[id_last] = 1

            try:
                self.affiliation_adjacency[link] += 1
            except KeyError:
                self.affiliation_adjacency[link] = 1

#             # Self-edge (need *some* value because dict will be queried for it later
#             link = (id0,id0)
#             try:
#                 self.affiliation_adjacency[link] += 0
#             except KeyError:
#                 self.affiliation_adjacency[link] = 1
#
        ''' The dictionary affiliation_adjacency assigns to each (source,target) tuple an integer weight.
            It is the sparse adjacency matrix of the inter-affiliation network.
        '''


        ''' The dictionary dict_timelines contains all the timelines with keys being integers.
            The dictionary dict_affiliation_to_timelines assigns to each affiliation id a list of id's of all timelines containing it
        '''











class MigrationAnalyzerUndirected(AffiliationAnalyzerUndirected):
    '''
    This class is used specifically for computing the geogoraphical transition
    graph. Here, in extract(), we add a weight of 1 to an edge if the two
    endpoint locations co-occur within a person's timeline.
    '''




    def extract(self):
        '''Override
        Computes the adjacency matrix between affiliation identifiers.
        The main product is self.affiliation_adjacency which is a dictionary {link:weight} where
        link is a tuple (ind1,ind2).
        The affiliation node indices are stored in two dictionaries self.dict_string_2_name  and
        self.dict_name_2_string.
        '''
        clustering = self.G.components()

        # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
        self.contributors_subgraphs = clustering.subgraphs()

        # The graph of the components: each component is contracted to one node
        Gbar = clustering.cluster_graph()
        print "number of subgraphs:", len(Gbar.vs)

        # show_histories_distribution(contributors_subgraphs); quit()

        # Loop through the subgraphs, i.e., resolved individual identities.
        for counter, g in enumerate(self.contributors_subgraphs):
            if self.debug:
                pass
#                 if counter > 10 : break


            list_affiliations = []

            # Loop through the nodes in each subgraph
            timeline = []
            dict_temp_affiliations = {}
            for counter_v, v in enumerate(g.vs):
                affiliation_index = self.settings['field_2_index'][self.affiliation.upper()]
#                 affiliation_index = 1 if self.affiliation == 'employer' else (6 if self.affiliation == 'occupation' else None)
                affiliation_identifier = self.dict_record_nodes[str(v['name'])]['data'][affiliation_index]
                if utils.bad_identifier(affiliation_identifier, type=self.affiliation):
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
            else:
                continue

            # Do some verbose logging
            if self.debug: print "Number of unique affiliation strings: %d" % len(list_affiliations)
            if   len(list_affiliations) > 10:
                self.list_sample_affiliation_groups.append([x[1] for x in list_affiliations])
                if self.debug:
                    for x in list_affiliations: print x[1]
                    print '======================================'


            set_affiliation_ids = {x[1] for x in timeline}
            for id0 in set_affiliation_ids:

                self_link = (id0, id0)
                try:
                    self.affiliation_adjacency[self_link] += 1
                except KeyError:
                    self.affiliation_adjacency[self_link] = 1

                for id1 in set_affiliation_ids:
                    if id0 == id1: continue
                    try:
                        self.affiliation_score[id0] += 1
                    except KeyError:
                        self.affiliation_score[id0] = 1

                    link = (id0, id1)
                    try:
                        self.affiliation_adjacency[link] += 1
                    except KeyError:
                        self.affiliation_adjacency[link] = 1




#
        ''' The dictionary affiliation_adjacency assigns to each (source,target) tuple an integer weight.
            It is the sparse adjacency matrix of the inter-affiliation network.
        '''


        ''' The dictionary dict_timelines contains all the timelines with keys being integers.
            The dictionary dict_affiliation_to_timelines assigns to each affiliation id a list of id's of all timelines containing it
        '''






def main():
    batch_id = 2398
    '''analyst = AffiliationAnalyzer(batch_id=batch_id, affiliation="occupation")
    state = analyst.settings["state"]
    analyst.load_data()
    analyst.extract()
    analyst.compute_affiliation_links()
    analyst.save_data(label=state)
    '''
    # analyst = AffiliationAnalyzerUndirected(state='newyork', batch_id=batch_id, affiliation="employer")
    analyst = AffiliationAnalyzerUndirected(state='newyork', batch_id=batch_id, affiliation="occupation")
    state = analyst.settings["state"]
    print state
    analyst.load_data()
    analyst.extract()
    analyst.compute_affiliation_links()
    analyst.save_data(label=state)


def loadAffiliationNetwork(label, data_path, affiliation):
    G = igraph.Graph.Read_GML(f=data_path + label + '-' + affiliation + '_graph.gml')
    return G


if __name__ == "__main__":
    main()


