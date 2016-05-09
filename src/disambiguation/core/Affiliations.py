'''
This module implements classes used to generate the affiliation (employer/occupation)
similarity graphs.
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
from .. import config
import igraph as ig


def get_committees():
    '''
    Get list of committee records. Here, we restrict to committees
    that are a) linked to house, senate or presidential candidates,
    and b) designated as  "Principal campaign committee of a candidate"
    '''

    retriever = FecRetriever(table_name='committee_master',
                      query_fields=['id', 'CMTE_ID', 'CAND_ID', 'CMTE_PTY_AFFILIATION'],
                      limit=(0, 100000000),
                      list_order_by='',
                      where_clause=" WHERE CMTE_TP in ('H','S','P') and CMTE_DSGN='P' ")
    retriever.retrieve()

    list_of_records = retriever.getRecords()
    return list_of_records








class AffiliationAnalyzer(object):
    '''
    A class that computes the affiliation co-occurrence/transition graphs
    based on the high-certainty pre-stage1 disambiguation.
    By the affiliation graphs we mean graphs that represent similarities
    between employer (or occupation) identifiers observed in the records.
    For instance, the employer graph could contain two nodes "Bank Of America"
    and "Chase" with a weight of 10 and "significance" 45. This means that
    from an elementary,high-certainty disambiguation we were able to observe
    10 instances where an individual reported their employer to be "Bank of
    America" in one record, and "Chase" in their next contribution, and we
    computed an MLF significance score of 45 for this tie. This information
    is used in later iterations of the disambiguation process to infer a link
    between to contribution records using the additional signal of similar
    affiliation (employer and/or occupation).
    '''


    def __init__(self, state="", batch_id=None, affiliation="employer"):
        self.debug = False
        self.state = state

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
        '''
        using get_committees(), create a  dict of committees
        '''
        list_committees = get_committees()
        for r in list_committees:
            self.dict_committees[r['CMTE_ID']] = r['CMTE_PTY_AFFILIATION']




    def get_party(self, CMTE_ID):
        '''
        Return the party affiliation of the committee id.
        0 for Democratic
        1 for Republican
        2 for Other.
        '''

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
        file_adjacency = open(config.data_path + file_label + str(self.batch_id) + '-adjacency.json')
        file_nodes = open(config.data_path + file_label + str(self.batch_id) + '-list_of_nodes.json')

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
        file_sample_affiliations = open(config.data_path + 'sample_' + self.affiliation + 's.json', 'w')
        file_sample_affiliations.write(json.dumps(self.list_sample_affiliation_groups))
        file_sample_affiliations.close()


    def load_settings(self, file_label=""):
        file_settings = open(config.data_path + file_label + str(self.batch_id) + '-settings.json')
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

        if self.affiliation == "employer":
            filename = config.affiliation_employer_file_template % label
        else:
            filename = config.affiliation_occupation_file_template % label

        self.G_affiliations.save(f=filename, format='gml')
        print "Saved affiliation graphs to file..."






    def compute_affiliation_links(self):
        '''
        Implement in subclasses
        '''





class AffiliationAnalyzerDirected(AffiliationAnalyzer):
    '''
    Experimental: implement the DIRECTED version of the MLF. Currently
    not used.
    '''



    def compute_affiliation_links(self):
        '''
        Overrides the original.
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
    '''
    A class that computes the pre-stage1 affiliation co-occurrence (tranition) graphs.
    The data needed for this is generated by L{generateAffiliationData} ()a stage1
    function), and saved to files. This data consists of a partial disambiguation
    of the records that can be linked together with high certainty because they have
    the exact same address. Using these high-certainty matches, we compile a
    timeline for each identified individual. Each affiliation identifier we find in
    a record in these timelines becomes a node in the affiliation graph. The edges
    of the graph have integer weights, counting the number of times there was a
    transition from one node to the other in a timeline.
    Then, using this integer weight, a significance score is computed using the
    Marginal Likelihood Filter (MLF) for each edge.
    This class uses the undirected version of the MLF.
    '''


    def compute_affiliation_links(self):
        '''
        For each edge in the L{self.G_affiliations} (which must be already
        computed) compute the significance score according to the Marginal
        Likelihood Filter (MLF).
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
        Computes the adjacency matrix between affiliation identifiers by compiling high-certainty
        timelines and counting the number of transitions in a timeline between two identifiers.
        The main product is self.affiliation_adjacency which is a dictionary {link:weight} where
        link is a tuple (ind1,ind2).
        The affiliation node indices are stored in two dictionaries self.dict_string_2_name and
        self.dict_name_2_string.
        There are two ways to provide the necessary data for this method to work:
        1- set L{self.G} which is a graph of matched records. This is the standard way, and is
        implemented in L{AffiliationAnalyzer.load_data()}
        2- set L{self.contributors_subgraphs} which is a list of graphs, where each graph is a
        connected component of L{self.G}.
        '''

        if not self.contributors_subgraphs:
            clustering = self.G.components()

            # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
            self.contributors_subgraphs = clustering.subgraphs()

            # The graph of the components: each component is contracted to one node
            Gbar = clustering.cluster_graph()
            print "number of subgraphs:", len(Gbar.vs)

        # show_histories_distribution(contributors_subgraphs); quit()


        # Count the number of node names not found in self.dict_record_nodes
        counter_records_not_found = 0

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
                try:
                    record_data = self.dict_record_nodes[str(v['name'])]['data']
                except KeyError:
                    counter_records_not_found += 1
                    continue
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


        ''' The dictionary affiliation_adjacency assigns to each (source,target) tuple an integer weight.
            It is the sparse adjacency matrix of the inter-affiliation network.
        '''


        ''' The dictionary dict_timelines contains all the timelines with keys being integers.
            The dictionary dict_affiliation_to_timelines assigns to each affiliation id a list of id's of all timelines containing it
        '''




        print "Number of node names not found in self.dict_record_nodes: ", counter_records_not_found








class AffiliationAnalyzerUndirectedPostStage1(AffiliationAnalyzerUndirected):
    '''
    Subclass of AffiliationAnalyzerUndirected. Generates a similar affiliation graph,
    but instead of the high-certainty matches found pre-stage1, it uses the stage1
    identities to link affiliation identifiers. For this, we need to override the
    L{load_data()} method so it loads the records using an FECretriever.
    Must set L{self.G}, L{self.dict_record_nodes}.
    '''
    def get_records(self):
        '''
        Retrieve all records for given state.
        '''
        state = self.state
        table_name = config.MySQL_table_state_combined % state
        retriever = FecRetriever(table_name=table_name)
        retriever.retrieve()
        self.list_of_records = retriever.getRecords()

    def load_data(self):
        '''
        For L{self.extract} to work, we need at the minimum two
        pieces of data: L{self.dict_record_nodes} as a container for
        all the relevant record data, and L{self.contributors_subgraphs}
        which is a list of graphs each one a set of vertices representing
        the records belonging to a stage1 identity.
        '''
        self.load_settings('')
        self.get_records()
        self.load_record_nodes()
        self.load_identities()
#         self.record_edge_list = json.load(file_adjacency)
#         self.dict_record_nodes = json.load(file_nodes)
#         self.G = igraph.Graph.TupleList(edges=self.record_edge_list)


    def load_identities(self):
        '''
        Load L{self.contributors_subgraphs} from the calculated
        stage1 identities. Then create L{self.contributors_subgraphs}
        '''
        try:
            idm = self.idm
        except:
            raise Exception('You must set self.idm to an IdentityManager instance')

        self.contributors_subgraphs = []
        # Iterate through all identities, for each one
        # create a Graph with each vertex named after
        # str(r_id).
        for identity, list_ids in idm.dict_identity_2_list_ids.iteritems():
            g = ig.Graph()
            g.add_vertices([str(x) for x in list_ids])
            self.contributors_subgraphs.append(g)



    def load_record_nodes(self):
        '''
        Populate L{self.dict_record_nodes} with important fields
        from all retrieved records.
        '''
        self.dict_record_nodes = {}
        dict_index = self.settings['field_2_index']
        for record in self.list_of_records:
            record_data = {}
            amount_index = dict_index['TRANSACTION_AMT']
            record_data[amount_index] = record['TRANSACTION_AMT']

            committee_index = dict_index['CMTE_ID']
            record_data[committee_index] = record['CMTE_ID']

            employer_index = dict_index['EMPLOYER']
            record_data[employer_index] = record['EMPLOYER']

            occupation_index = dict_index['OCCUPATION']
            record_data[occupation_index] = record['OCCUPATION']

            date_index = dict_index['TRANSACTION_DT']
            record_data[date_index] = record['TRANSACTION_DT']

            self.dict_record_nodes[str(record.id)] = {'data': record_data}



    def set_idm(self, idm):
        '''
        Assign an IdentityManager to this instance.
        '''
        self.idm = idm




    def load_settings(self, file_label):
        '''
        Define the L{self.settings} instance that defines the
        schema of the nodes dictionary. This is completely arbitrary:
        the same settings will be used to generate dict_records_nodes,
        and to interpret it.
        @param file_label: dummy.
        '''
        self.settings = {'field_2_index' : {'TRANSACTION_DT' : 0,
                                            'TRANSACTION_AMT' : 1,
                                            'CMTE_ID' : 2,
                                            'EMPLOYER': 3,
                                            'OCCUPATION': 4}
                        }


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

        if self.affiliation == "employer":
            filename = config.affiliation_poststage1_employer_file_template % label
        else:
            filename = config.affiliation_poststage1_occupation_file_template % label

        self.G_affiliations.save(f=filename, format='gml')
        print "Saved affiliation graphs to file..."





class AffiliationAnalyzerUndirectedPostStage1_COOCCURRENCE(AffiliationAnalyzerUndirectedPostStage1):
    '''
    Subclass of AffiliationAnalyzerUndirectedPostStage1. Generates a similar affiliation graph,
    but instead of generating the transition graph between affiliation labels, it generates
    the bipartite person-affiliation graph that can be used to derive the strictly
    "co-occurrence network" of affiliations in individuals.
    '''

    def extract(self):
        '''Override
        Compute the list of sets of affiliation identifiers. This is for the
        purpose of compiling the full bipartite person/affiliation graph from
        which a strictly "co-occurrence" network can be compiled.

        The main product is self.list_sets which is a set where each item corresponds to
        one connected component of self.contributors_subgraph and is a set of
        affiliation identifiers found in that identity.
        There are two ways to provide the necessary data for this method to work:
        1- set L{self.G} which is a graph of matched records. This is the standard way, and is
        implemented in L{AffiliationAnalyzer.load_data()}
        2- set L{self.contributors_subgraphs} which is a list of graphs, where each graph is a
        connected component of L{self.G}.
        '''
        def satisfies_condition(s):
            '''
            Our condition for including an affiliation identfier
            in the timeline. This data is used for analyzing the
            backbone of the affiliation networks, so we throw away
            problematic strings liberally.
            '''
            # Only include single-word strings
            if re.findall(r' |\.|\,|\||\\|\/|\-|\t', s):
                return False
            else:
                return True

        if not self.contributors_subgraphs:
            clustering = self.G.components()

            # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
            self.contributors_subgraphs = clustering.subgraphs()

            # The graph of the components: each component is contracted to one node
            Gbar = clustering.cluster_graph()
            print "number of subgraphs:", len(Gbar.vs)

        # show_histories_distribution(contributors_subgraphs); quit()


        # Count the number of node names not found in self.dict_record_nodes
        counter_records_not_found = 0

        list_sets = []

        # Loop through the subgraphs, i.e., resolved individual identities.
        for counter, g in enumerate(self.contributors_subgraphs):
            # Loop through the nodes in each subgraph
            timeline = set()

            for counter_v, v in enumerate(g.vs):
                try:
                    record_data = self.dict_record_nodes[str(v['name'])]['data']
                except KeyError:
                    counter_records_not_found += 1
                    continue
                affiliation_index = self.settings['field_2_index'][self.affiliation.upper()]
                affiliation_identifier = record_data[affiliation_index]

                if utils.bad_identifier(affiliation_identifier, type=self.affiliation):
                    if self.debug: print affiliation_identifier
                    continue

                if satisfies_condition(affiliation_identifier):
                    timeline.add(affiliation_identifier)
            if timeline and len(timeline) > 1:
                list_sets.append(timeline)
        print "Number of node names not found in self.dict_record_nodes: ", counter_records_not_found
        self.list_sets = list_sets


    def save_data(self, label=""):
        if label == "":
            label = self.batch_id

        if self.affiliation == "employer":
            filename = config.affiliation_poststage1_employer_file_template % ("co-occurrence-" + label)
        else:
            filename = config.affiliation_poststage1_occupation_file_template % ("co-occurrence-" + label)

        with open(filename, 'w') as f:
            for set_affiliations in self.list_sets:
                f.write('|'.join(set_affiliations) + "\n")

        print "Saved affiliation cooccurrence data to file..."






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


