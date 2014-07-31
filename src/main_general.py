#! /usr/bin/python
# This program analyzes the full database for a given state. That is, not just the <state>_addresses
# 
# The <state>_addresses tables are roughly 0.25 the size of their corresponding <state> tables. Why?
# My explanation is that lots of records that have unique identifier pairs in the <state> table, 
# occur multiple times in the contributor_addresses table, and therefore don't appear in its unique
# version, namely contributor_addresses_unique.
''' TODO:
    1- (DONE: <state>_combined) We need a new table that combines <state> and <state>_addresses as follows:
        The table structure is like <state>_addresses, but it contains all the records found in <state>.
        For records that have address fields in <state>_addresses, use those. For others, use null.

    2- This table will be analyzed in this code.

    3- Add additional fields to each record:
        Name frequency, Timeline period (if exists), Size of neighborhood in employer graph?, Centrality in Employer graph? Size of neighborhood in occupation graph

    4- Identify the right set of features of the records from this table to use with Disambiguator.
        Candidates: Name, Address, Employer, Occupation, CMTE_ID (may be confounding)

    5- Define "possible match". Identifying possible matches will be the first stage of disambiguation. We will refine our knowledge starting from these "possible matches".
        ? Name is similar (specify)
        ? Address is same
        ? Employer is
        '''
'''
Strategy:
    - Using <state>_addresses, find possible matches (use Disambiguator)
    - Using the results, use AffiliationAnalyzer to derive network of affiliations
    -
    - Query <state>_combined. Contains all records from <state>, and for some, additional data from <state>_addresses
    - Find possible matches using Disambiguator and additional attributes: frequencies, affiliations, etc.
    - A different comparison function is needed.
    -
'''
''' Notes:
    1- The <state> tables are 3~4 times as large as <state>_addresses tables.
    2- The <state>_address and <state>_combined tables don't have all the FIRST_NAMEs and LAST_NAMEs so
        if I use these tables to get names, some records won't have names.
'''
# import nltk
# establish and return a connection to the MySql database server

from collections import OrderedDict
import igraph
import json
import os
import pprint
import re
import sys
import time
from states import *
import multiprocessing 
import random
import datetime
import glob

from Affiliations import AffilliationAnalyzer
from Disambiguator import Disambiguator
from Database import FecRetriever
from Tokenizer import Tokenizer, TokenizerNgram

import pandas as pd

def find_all_in_list(regex, str_list):
    ''' Given a list of strings, str_list and a regular expression, regex, return a dictionary with the
        frequencies of all mathces in the list.'''
    dict_matches = {}
    for s in str_list:
#         s_list = re.findall(r'\b\w\b', s)
        s_list = re.findall(regex, s)
        for s1 in s_list:
            if s1 in dict_matches:
                dict_matches[s1] += 1 
            else:
                dict_matches[s1] = 1
    return dict_matches

 

def get_next_batch_id():
    f = open('../config/batches.list')
    s = f.read()
    f.close() 
    i = int(s)
    f = open('../config/batches.list', 'w')
    f.write(str(i + 1))
    f.close()
    return(str(i))



''' Loads the saved output of AffiliatoinAnalyzer from file: the affiliation network.
    It also adds a new attribute to the graph instance that contains a dictionary from
    affiliation identifier strings to the index of their corresponding vertex in the graph object.
'''
def loadAffiliationNetwork(label, data_path, affiliation):
    try:
        G = igraph.Graph.Read_GML(f=data_path + label + '-' + affiliation + '_graph.gml')
        dict_string_2_ind = {v['label']:v.index for v in G.vs}
        G.dict_string_2_ind = dict_string_2_ind
    except IOError:
        print "ERROR: Affiliation Network data not found."
        G = None
    
    # Not really necessary any more. I construct a {string: index} dictionary from the loaded Graph myself. 
    # metadata = json.load(open(data_path + label + '-' + affiliation + '-metadata.json'))
    return G

    


''' One version of main().
    Operates on <state>_addresses tables in order to find high-certainty identities so that
    affiliation networks can be generated.
    The output of this method will be the data files which can be loaded by loadAffiliationNetwork().
    The comparison method used by Records here should be strict.
    '''
def generateAffiliationData(state=None, affiliation = None,record_limit = (0,5000000)):
    '''
    1- Pick a list of fields, pick a table and instantiate an FecRetriever object to fetch those fields from the table.
        This produces a list of Record objects.
    2- Instantiate a Tokenizer object, and pass the list of records to the Tokenizer. Tokenize them, and retrieve the
        updated list of records. These records now have a reference to the Tokenizers TokenData object, and contain
        an attribute called record.vector.
    3- Instantiate a Disambiguator object and pass to it the list of records. This Disambiguator will use the vector
        attributes of the records to find a set of approximate nearest neighbors for each one. The result is an adjacency
        matrix.
    4- Instantiate a Project object, and set various parameters to it as instance variables. For example, the Disambiguator
        object defined above is bound to the Project as an instance variable.
        This Project object will then do the book keeping: saves a settings file, saves the adjacency matrix to a file,
        saves a json version of all records to a file, etc.
    5- The json files saved by Project, namely the adjacency matrix and the list of records, will be used by the code
        defined in the Affiliations module to extract
    '''  
    batch_id = get_next_batch_id()
    project = Project(batch_id=batch_id)
    
    if state:
        
        param_state = state
    else: 
        param_state = 'newyork_addresses'
        
    print "Analyzing data for state: ", param_state 
    
    table_name = param_state + "_combined"
  
    # If this is for a multi-state table
    if param_state == "multi_state":
        table_name = "multi_state_combined"
    project.putData('param_state' , param_state)

    
    record_start = record_limit[0]
    record_no = record_limit[1]
    

    project.putData('batch_id' , batch_id)

    
    time1 = time.time()
    
#    list_tokenized_fields = ['NAME','FIRST_NAME', 'LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    project.putData("list_tokenized_fields", list_tokenized_fields)
    
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP','id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)
    
    all_fields = list_tokenized_fields + list_auxiliary_fields 
    project.putData('all_fields' , all_fields)

    
    # I BELIEVE THESE ARE UNNECCESSARY
    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]
    
    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]    
    
    
    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.putData("index_2_field", index_2_field)
    
    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.putData("field_2_index", field_2_index)
    
    
    
    
    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE", "CMTE_ID"],
                      where_clause=" WHERE ENTITY_TP='IND' ")
    retriever.retrieve()
    project.putData("query", retriever.getQuery())
    
    list_of_records = retriever.getRecords()
    
#     # A list(1) of lists(2)  where each list(2) corresponds to one of the records returned by MySQL
#     # and it contains only the "identifier" fields of that record. This is the main piece of data processed by this program.
#     list_of_records_identifier = [[record[index] for index in index_list_tokenized_fields] for record in tmp_list]
#     
#     # Same as above, except each list(2) consists of the auxiliary columns of the record returned by MySQL.
#     list_of_records_auxiliary = [[record[index] for index in index_auxiliary_fields] for record in tmp_list]  
    
    
    tokenizer = TokenizerNgram()
    project.tokenizer = tokenizer
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(list_tokenized_fields)
    tokenizer.tokenize()
    
    tokenizer.tokens.save_to_file(project["data_path"] + batch_id + "-tokendata.pickle")
    list_of_records = tokenizer.getRecords()
    print len(tokenizer.tokens.token_2_index.keys())
    
    
    ''' HERE WE DON'T LOAD AFFILIATION DATA BECAUSE THAT'S WHAT WE WANT TO PRODUCE! '''

    
    # Unnecessary
#     project.list_of_records = list_of_records
    
    # dimension of input vectors
    dim = tokenizer.tokens.no_of_tokens

    D = Disambiguator(list_of_records, dim, matching_mode="strict")
    project.D = D
    D.project = project
    
    
    # desired dimension (length) of hashes
    hash_dim = 20
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 40
    
    
    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))
    
    print 'Analyze...'
    t1 = time.time()
    # compute the hashes
    D.compute_LSH_hash(hash_dim)
    D.save_LSH_hash(batch_id=batch_id)
    D.compute_similarity(B1=B, m=no_of_permutations , sigma1=None)
    
    # D.show_sample_adjacency()

    t2 = time.time()
    print 'Done...'
    print t2 - t1
   
    # project.D.imshow_adjacency_matrix(r=(0, record_no))
    
    print 'Saving adjacency matrix to file...'
    print 'Printing list of identifiers and text of adjacency matrix to file...'
    project.save_data(with_tokens=False)
    print 'Done...'
    
    time2 = time.time()
    print time2 - time1




    project.log('MESSAGE', 'Computing affiliation networks...')
    project.saveSettings()

    if affiliation is None or affiliation == 'occupation':
        try:
            analyst = AffilliationAnalyzer(batch_id=batch_id, affiliation="occupation")
            project.log('MESSAGE', 'AffiliationAnalyzer created...')
            
            state = analyst.settings["param_state"]
            analyst.load_data()
            analyst.extract()
            analyst.compute_affiliation_links()
            project.log('MESSAGE', 'Affiliation links computed...')

            analyst.save_data(label=state)
            project.log('MESSAGE', 'Affiliation data saved...')
        #     analyst.save_data_textual(label=state)
        except Exception as e:
            print e

    if affiliation is None or affiliation == 'employer':
        try:
            analyst = AffilliationAnalyzer(batch_id=batch_id, affiliation="employer")
            project.log('MESSAGE', 'AffiliationAnalyzer created...')
            
            state = analyst.settings["param_state"]
            analyst.load_data()
            analyst.extract()
            analyst.compute_affiliation_links()
            project.log('MESSAGE', 'Affiliation links computed...')

            analyst.save_data(label=state)
            project.log('MESSAGE', 'Affiliation data saved...')
        except Exception as e:
            print e



def disambiguate_main(state, record_limit=(0, 5000000)):  
    '''
    1- Pick a list of fields, pick a table and instantiate an FecRetriever object to fetch those fields from the table.
        This produces a list of Record objects.
    2- Instantiate a Tokenizer object, and pass the list of records to the Tokenizer. Tokenize them, and retrieve the
        updated list of records. These records now have a reference to the Tokenizers TokenData object, and contain
        an attribute called record.vector.
    3- Instantiate a Disambiguator object and pass to it the list of records. This Disambiguator will use the vector
        attributes of the records to find a set of approximate nearest neighbors for each one. The result is an adjacency
        matrix.
    4- Instantiate a Project object, and set various parameters to it as instance variables. For example, the Disambiguator
        object defined above is bound to the Project as an instance variable.
        This Project object will then do the book keeping: saves a settings file, saves the adjacency matrix to a file,
        saves a json version of all records to a file, etc.
    5- The json files saved by Project, namely the adjacency matrix and the list of records, will be used by the code
        defined in the Affiliations module to extract
    '''  
    batch_id = get_next_batch_id()
    project = Project(batch_id=batch_id)
    
    if state:
        param_state = state
    else:
        # TODO: this is the table name. Which table should be used for the final disambiguation?
        # Answer: for the state level, <state>_combined. For the whole country, probably the union
        # of all state tables.
        
        param_state = 'newyork_combined'
    print "Analyzing data for state: ", param_state 
    
    table_name = param_state + "_combined"
    
    project.putData('param_state' , param_state)

    
    
    
    record_start = record_limit[0]
    record_no = record_limit[1]

    project.putData('batch_id' , batch_id)

    
    time1 = time.time()
    
#    list_tokenized_fields = ['NAME','FIRST_NAME', 'LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
    list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    project.putData("list_tokenized_fields", list_tokenized_fields)
    
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)
    
    all_fields = list_tokenized_fields + list_auxiliary_fields 
    project.putData('all_fields' , all_fields)


    # I BELIEVE THESE ARE UNNECCESSARY
    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]
    
    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]    
    
    
    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.putData("index_2_field", index_2_field)
    
    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.putData("field_2_index", field_2_index)
    
    
    
    
    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
#                       where_clause=" WHERE ENTITY_TP='IND' ")
                      where_clause=" ")
    retriever.retrieve()
    project.putData("query", retriever.getQuery())
    
    list_of_records = retriever.getRecords()
    
#     # A list(1) of lists(2)  where each list(2) corresponds to one of the records returned by MySQL
#     # and it contains only the "identifier" fields of that record. This is the main piece of data processed by this program.
#     list_of_records_identifier = [[record[index] for index in index_list_tokenized_fields] for record in tmp_list]
#     
#     # Same as above, except each list(2) consists of the auxiliary columns of the record returned by MySQL.
#     list_of_records_auxiliary = [[record[index] for index in index_auxiliary_fields] for record in tmp_list]  
    
    
    tokenizer = TokenizerNgram()
    project.tokenizer = tokenizer
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(list_tokenized_fields)
    tokenizer.tokenize()
    
    tokenizer.tokens.save_to_file(project["data_path"] + batch_id + "-tokendata.pickle")
    list_of_records = tokenizer.getRecords()
    

    
    ''' Load affiliation graph data.
        These graph objects also contain a dictionary G.dict_string_2_ind
        for faster vertex access based on affiliation string.

        For affiliation data to exist, one must have previously executed
      When used via self.compare(), a positive value is
    interpreted as True.   Affiliations on the <state>_addresses data.
    '''
    
    G_employer = loadAffiliationNetwork(param_state, project['data_path'], 'employer')
    if G_employer:
        for record in list_of_records:
            record.G_employer = [G_employer]
    else:
        print "WARNING: EMPLOYER network not found."
        project.log('WARNING', param_state + " EMPLOYER network not found.")

    
    
    G_occupation = loadAffiliationNetwork(param_state, project['data_path'], 'occupation')
    if G_occupation:  
        for record in list_of_records:
            record.G_occupation = [G_occupation]
    else:
        print "WARNING: OCCUPATION network not found."
        project.log('WARNING', param_state + " OCCUPATION network not found.")

    
    
    
    
    
    
    print len(tokenizer.tokens.token_2_index.keys())
    
    
    project.list_of_records = list_of_records
    
    # dimension of input vectors
    dim = tokenizer.tokens.no_of_tokens

    D = Disambiguator(list_of_records, dim, matching_mode='thorough')
    D.tokenizer = tokenizer
    project.D = D
    D.project = project
    
    
    # desired dimension (length) of hashes
    hash_dim = 20
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 20
    
    
    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))

    
#     self.D = Disambiguator(self.list_of_vectors, self.tokens.index_2_token, self.tokens.token_2_index, dim, self.batch_id)
    
    
    print 'Analyze...'
    t1 = time.time()
    # compute the hashes
    D.compute_LSH_hash(hash_dim)
    D.save_LSH_hash(batch_id=batch_id)
    D.compute_similarity(B1=B, m=no_of_permutations , sigma1=None)
    
#     project.save_data_textual(with_tokens=False, file_label="before")

    D.generate_identities()
    D.refine_identities()
    
   
    



#     print json.dumps(D.dict_already_compared_pairs.keys(),indent=2)
#     quit()
    # D.show_sample_adjacency()

    t2 = time.time()
    print 'Done...'
    print t2 - t1

    # project.D.imshow_adjacency_matrix(r=(0, record_no(
        
    print 'Saving adjacency matrix to file...'
    print 'Printing list of identifiers and text of adjacency matrix to file...'
#     project.save_data(with_tokens=False)
    project.save_data_textual(with_tokens=False, file_label=param_state)
    print 'Done...'
    
    time2 = time.time()
    print time2 - time1

    project.saveSettings()
    
    project.dump_full_adjacency()
    
    
    return project





class Project(dict):
    def __init__(self, batch_id):
        self["batch_id"] = batch_id
        self["data_path"] = os.path.expanduser('~/data/FEC/')
        self["logfilename"] = '../records/' + str(self["batch_id"]) + '.record'
        self["logfile"] = open(self["logfilename"], 'w', 0)
        self["messages"] = []
        self["list_tokenized_fields"] = []
        self["list_auxiliary_fields"] = []
    
    
        
    def saveSettings(self):
        settings = {}
        for item in self.keys():
            if item not in ['list_of_records', "D", "tokenizer"]:
#             if True:
                try:
                    tmp = json.dumps(self[item])
                    settings[item] = self[item]
                except TypeError:
                    print "WWARNING: ", item, " not serializable. Skiping." 
        f = open(self["data_path"] + self["batch_id"] + '-settings.json', 'w')
        f.write(json.dumps(settings, indent=4))
        f.close()
        
        
       
        
    def putData(self, key, value):
        self[key] = value
    
    def log(self, key, value):
#         self.messages.append((key, value))
        self["logfile"].write("%s : %s\n" % (key, value))  # write without buffering
#         self.logfile.close()
#         self.logfile = open(self.logfilename, 'w')
    
    def setBatchId(self, batch_id):
        self["batch_id"] = batch_id
        self.log('Batch ID' , self.batch_id)

        
      

    ''' Take the adjacency matrix resulting from the Disambiguator object,
    and write it to file in edgelist (.edges) and .json formats '''
    def save_graph_to_file(self, list_of_nodes=[]):
        filename_json = self["data_path"] + self["batch_id"] + '-adjacency.json'
        filename_edgelist = self["data_path"] + self["batch_id"] + '-adjacency.edges'
        f_json = open(filename_json, 'w') 
        f_edgelist = open(filename_edgelist, 'w') 

        if  not self.D.index_adjacency: return 
        if not list_of_nodes: list_of_nodes = range(len(self.D.list_of_records))
        nmin = list_of_nodes[0]
        list_of_links = []
        for node1 in list_of_nodes:
            for node2 in self.D.index_adjacency[node1]:
                list_of_links.append((node1 - nmin, node2 - nmin))
                f_edgelist.write(str(node1 - nmin) + ' ' + str(node2 - nmin) + "\n")
        f_json.write(json.dumps(list_of_links))
        f_json.close()
        f_edgelist.close()

#     def save_graph_to_file(self, list_of_nodes=[]):
#         if  not self.D.index_adjacency: return 
#         if not list_of_nodes: list_of_nodes = range(len(self.list_of_records))
#         nmin = list_of_nodes[0]
#         for node1 in list_of_nodes:
#             for node2 in self.D.index_adjacency[node1]:
    
    
    # Computes the full adjacency matrix from the D.set_of_persons and dumps it to a text file as edgelist
    def dump_full_adjacency(self):
        print "writing full adjacency to file... "
        filename_edgelist = self["data_path"] + self["batch_id"] + '-adjacency.edges'
        f = open(filename_edgelist, 'w')
        for person in self.D.set_of_persons:
            for record1 in person.set_of_records:
                for record2 in person.set_of_records:
                    if record1 is not record2:
                        f.write(str(record1.id) + " " + str(record2.id) + "\n")
        f.close()
                        
        pass  

    
    
    def set_list_of_records_auxiliary(self, tmp_list):
        ''' This functions sets the list of auxiliary records associated with the items in list_of_records_identifier'''
        self.list_of_records_auxiliary = tmp_list
    def set_list_of_records_identifier(self, list_of_records_identifier):
        ''' This functions sets the main list of strings on which the similarity analysis is performed'''
        self.list_of_records_identifier = list_of_records_identifier
    
    def save_data_textual(self, with_tokens=False, file_label=""):
        css_code = "table{border-collapse:collapse;\
                    padding:5px;\
                    font-family:sans;\
#                     width:100%;\
                    font-size:10px;\
                    border:dotted thin #efefef;}\
                    td{padding:5px;\
                    border:dotted thin #efefef;} "
        filename1 = self["data_path"] + file_label + self["batch_id"] + '-adj_clusters.json'
        filename2 = self["data_path"] + file_label + self["batch_id"] + '-adj_clusters.html'

        f1 = open(filename1, 'w')
        f2 = open(filename2, 'w')
        f2.write("<head><style>"
                 + css_code

                 + "</style>"
                 + "</head>")
        
        list_tokens = []
        dict_tokens = {}
        for record in self.D.list_of_records:
            dict_tokens[record.id] = self.D.tokenizer._get_tokens(record, self["list_tokenized_fields"])
            
        print len(dict_tokens), len(self.D.list_of_records)
#         quit()


        # how many blocks at a time to dump to file
        page_size = 20;
        
        if self.D:
           
            list_id = []
            dataframe_data = []
            
            # Old version, before D.set_of_persons was implemented
            # for g in sorted(self.persons_subgraphs, key=lambda g: min([int(v['name']) for v in g.vs])):
            #     for v in sorted(g.vs, key=lambda v:int(v['name'])):
            #           index = int(v['name'])
            #           r = self.list_of_records [index]
            
            # Where in self["list_tokenized_fields"] is the date field? Used below
            time_index = self["list_tokenized_fields"].index('TRANSACTION_DT')
            
            person_counter = 0 
            for person in sorted(list(self.D.set_of_persons), key=lambda person:min([r['NAME'] for r in person.set_of_records ])):
                new_block = []
                for r in sorted(list(person.set_of_records), key=lambda record : record['NAME']):
                
                
                    
#                     index = r.index
                    list_id.append(r.id)
                    record_as_list_tokenized = [r[field] for field in self["list_tokenized_fields"]]
                    record_as_list_auxiliary = [r[field] for field in self["list_auxiliary_fields"]]
                    
#                     print len(list_tokens), index
                    tmp_tokens = dict_tokens[r.id]
                    tokens_str = [str(x) for x in tmp_tokens]
                    
                    # new_row = record_as_list_tokenized + [r['N_first_name'], r['N_last_name'], r['N_middle_name']]

                    # With tokens
                    # new_row = record_as_list_tokenized + [' '.join(tokens_str)] + [r['N_first_name'], r['N_last_name'], r['N_middle_name']]
                    
                    # Without tokens
                    # new_row = record_as_list_tokenized + [r['N_first_name'], r['N_last_name'], r['N_middle_name']]
                    
                    # without normalized names
                    new_row = record_as_list_tokenized + [r['N_address']]
                    
                    new_row = ["" if s is None else s.encode('ascii', 'ignore') if isinstance(s,unicode) else s  for s in new_row ]
                    new_block.append(new_row)

                    
                    if with_tokens:
                        s1 = "%d %s        %s\n" % (r.id, record_as_list_tokenized , '|'.join(tokens_str))
                    else:
                        s1 = "%d %s\n" % (r.id, record_as_list_tokenized)
#                     f1.write(s1)
                new_block = sorted(new_block, key=lambda row:row[time_index])
                dataframe_data += new_block
                list_id += ["" for i in range(3)]
                dataframe_data += [["" for i in range(len(dataframe_data[0]) - 1)] + ["|"] for j in range(3)]
                
                person_counter += 1
                
                # Save a group of blocks to file
                if person_counter % page_size == 0:
                    df = pd.DataFrame(dataframe_data, index=list_id, columns=self["list_tokenized_fields"]+['N_address'])
                    f1.write(df.to_string(justify='left').encode('ascii','ignore'))
                    f2.write(df.to_html().encode('ascii','ignore'))
                    f2.write("<br/><br/>")
                    
                    # Reset the output buffer
                    list_id = []
                    dataframe_data = []
                    


                
#                 f1.write('\n' + separator + '\n')   

#             df = pd.DataFrame(dataframe_data, index=list_id, columns=self["list_tokenized_fields"]+['N_first_name', 'N_last_name', 'N_middle_name'])
#             df = pd.DataFrame(dataframe_data, index=list_id, columns=self["list_tokenized_fields"] + ['tokens']+['N_first_name', 'N_last_name', 'N_middle_name'])
            
            # if there's a fraction of a page left at the end, write that too. 
            if dataframe_data:
                df = pd.DataFrame(dataframe_data, index=list_id, columns=self["list_tokenized_fields"]+['N_address'])
                f1.write(df.to_string(justify='left').encode('ascii','ignore'))
    
                f2.write(df.to_html().encode('ascii','ignore'))
                f2.write("<br/><br/>")

            f1.close()
            f2.close()
                
                
                    
                    
            
            

    
    def save_data(self, r=[], verbose=False, with_tokens=False):
            ''' This function does three things:
                1- saves a full description of the nodes with all attributes in json format to a file <batch_id>-list_of_nodes.json
                   This file, together with the <batch-id>-adjacency.txt file provides all the information about the graph and its
                   node attributes.
                2- saves a formatted text representation of the adjacency matrix with identifier information
                3- saves a formatted text representation of the adjacency matrix with auxiliary field information.
            '''
            
            # Save the adjacency matrix to file in both edgelist and json formats
            self.save_graph_to_file()
            
            filename1 = self["data_path"] + self["batch_id"] + '-adj_text_identifiers.json'
            filename2 = self["data_path"] + self["batch_id"] + '-adj_text_auxiliary.json'
            filename3 = self["data_path"] + self["batch_id"] + '-list_of_nodes.json'
            if self.D and self.D.index_adjacency:
    #             separator = '----------------------------------------------------------------------------------------------------------------------'
                separator = '______________________________________________________________________________________________________________________'
                pp = pprint.PrettyPrinter(indent=4)
#                 pp.pprint(self.D.index_adjacency)
    
                n = len(self.D.list_of_records)
                if r:
                    save_range = range(max(0, r[0]), min(n, r[1]))
                else: 
                    save_range = range(len(self.D.list_of_records))
    
                file1 = open(filename1, 'w')
                file2 = open(filename2, 'w')
                file3 = open(filename3, 'w')
                dict_all3 = {}
                
                list_tokens = []
                for i in save_range:
                    list_tokens.append(self.tokenizer._get_tokens(self.D.list_of_records [i], self["list_tokenized_fields"]))
                    
                for i in save_range:

                    tmp_tokens = list_tokens[i]
                    tokens_str = [str(x) for x in tmp_tokens]
                    tokens = {x[0]:x[1] for x in tmp_tokens} 
                    
                    record_as_list_tokenized = [self.D.list_of_records [i][field] for field in sorted(self["list_tokenized_fields"])]
                    record_as_list_auxiliary = [self.D.list_of_records [i][field] for field in sorted(self["list_auxiliary_fields"])]

#                     dict_all3[i] = {'ident':record_as_list_tokenized, 'aux':record_as_list_auxiliary, 'ident_tokens':tokens}
                    # print self["all_fields"][0]
                    dict_all3[i] = {'data':[self.D.list_of_records[i][field] for field in self["all_fields"]],
                                             'ident_tokens':tokens}
                    

                    if with_tokens:
                        s1 = "%d %s        %s\n" % (i, record_as_list_tokenized , '|'.join(tokens_str))
                    else:
                        s1 = "%d %s\n" % (i, record_as_list_tokenized)

                    s2 = "%d %s \n" % (i, record_as_list_auxiliary)
                    file1.write(separator + '\n' + s1)   
                    file2.write(separator + '\n' + s2)
                    for j in sorted(self.D.index_adjacency[i], key=lambda k:self.D.list_of_records [k]['TRANSACTION_DT']):
#                         record_as_list_tokenized__2 = [self.list_of_records[j][field] for field in sorted(self.list_of_records[j].keys())]
                        record_as_list_tokenized__2 = [self.D.list_of_records [j][field] for field in sorted(self["list_tokenized_fields"])]
                        record_as_list_auxiliary__2 = [self.D.list_of_records [j][field] for field in sorted(self["list_auxiliary_fields"])]
                        
                        tmp_tokens = [str(x) for x in list_tokens[j]]

                        tokens_str = [str(x) for x in tmp_tokens]
                        tokens = {x[0]:x[1] for x in tmp_tokens} 
                        
                        if with_tokens:
                            s1 = "    %d %s        %s\n" % (j, record_as_list_tokenized__2 , '|'.join(tokens_str))
                        else:
                            s1 = "    %d %s\n" % (j, record_as_list_tokenized__2)
                            
                        s2 = "    %d %s \n" % (j, record_as_list_auxiliary__2)
                        file1.write(s1)   
                        file2.write(s2)    
    
                file3.write(json.dumps(dict_all3))    
                
                file1.close()
                file2.close()
                file3.close()
                
            


def worker(conn):
    data = conn.recv()
    proc_name = multiprocessing.current_process().name
    print proc_name, data
    
    for state in data:
        #print state
        #disambiguate_main(state,record_limit = (0,1000))
        generateAffiliationData(state)   
        print "="*70, "\n" + state + " done." + str(datetime.datetime.now()) + "\n" + "="*70 
    # time.sleep(random.randint(1,10))
    # conn.send(proc_name+" Done!")    
    
#     generateAffiliationData('alaska')   
#     disambiguate_main('alaska')
    
#     generateAffiliationData('newyork')   
#     disambiguate_main('newyork')
    
#     generateAffiliationData('delaware')   
#     disambiguate_main('delaware')
    
#    generateAffiliationData('california')   
#    disambiguate_main('california')
    
#     generateAffiliationData("multi_state")   
#     disambiguate_main('multi_state')








if __name__ == "__main__":



    
#     generateAffiliationData('alaska',affiliation = "employer",record_limit = (0,500000))   
    disambiguate_main('delaware',record_limit = (0,1000))
    quit()

    list_states = []

    # Use custom list of states
    list_states = ['california','texas','marshallislands','palau','georgia','newjersey']

    list_jobs = []

    # if not specified,  load all states
    if not list_states:
        list_states = sorted(dict_state.values())


    set_states = set(list_states)

    N =  len(list_states)

    # No more than 10 processes
    number_of_processes = min(N,10)

    
    
    dict_states = {}
    
    dict_conns = {}


    proc_id = 0
    while set_states:
        if proc_id not in dict_states: dict_states[proc_id] = set()
        dict_states[proc_id].add(set_states.pop())
        proc_id +=1
        proc_id = proc_id %number_of_processes

    for id in dict_states:
        print id, dict_states[id]

        
    for id in dict_states:
        # queue = multiprocessing.Queue()
        conn_parent, conn_child = multiprocessing.Pipe()
        dict_conns[id] = (conn_parent, conn_child)        


        p = multiprocessing.Process(target=worker, name=str(id), args=(conn_child,))

        # set process as daemon. Let it run in the background
        # p.daemon = True

        #list_jobs.append(p)
        conn_parent.send(dict_states[id])
        p.start()
    

    
    # for id in dict_states:
    #    (conn_parent,conn_child) = dict_conns[id] 
    #    print conn_parent.recv()
        
        

 