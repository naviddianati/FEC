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

import json
import os
import pprint
import re
import sys
import time

from Disambiguator import Disambiguator
from Retriever import FecRetriever
from Tokenizer import Tokenizer, TokenizerNgram


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


    
    
def main():  
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
        object defined above is assigned to the Project as an instance variable.
        This Project object will then do the book keeping: saves a settings file, saves the adjacency matrix to a file,
        saves a json version of all records to a file, etc.
    5- The json files saved by Project, namely the adjacency matrix and the list of records, will be used by the code
        defined in the Affiliations module to extract
    '''  
    batch_id = get_next_batch_id()
    project = Project(batch_id=batch_id)
    
    pp = pprint.PrettyPrinter(indent=4)
    if len(sys.argv) > 1:
        param_state = sys.argv[1]
    else:
        param_state = 'newyork_addresses'
    print "Analyzing data for state: ", param_state 
    
    project.log('param_state' , param_state)

    
    record_start = 1
    record_no = 5000

    project.log('Batch ID' , batch_id)

    
    time1 = time.time()
    
#    list_tokenized_fields = ['NAME','FIRST_NAME', 'LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1', 'OCCUPATION']
    project.list_tokenized_fields = list_tokenized_fields
    
    list_auxiliary_fields = ['TRANSACTION_DT', 'EMPLOYER', 'TRANSACTION_AMT', 'CITY', 'CMTE_ID', 'ENTITY_TP']
    project.list_auxiliary_fields = list_auxiliary_fields
    
    all_fields = list_tokenized_fields + list_auxiliary_fields 
    project.all_fields = all_fields
    project.log('All Fields' , ",".join(all_fields))


    # I BELIEVE THESE ARE UNNECCESSARY
    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]
    
    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]    
    
    
    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.index_2_field = index_2_field
    
    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.field_2_index = field_2_index
    
    
    
    
    retriever = FecRetriever(table_name=param_state,
                      query_fields=all_fields,
                      limit=(record_start, record_start + record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE", "CMTE_ID"],
                      where_clause=" WHERE ENTITY_TP='IND' ")
    retriever.retrieve()
    project.query = retriever.getQuery()
    
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
    
    tokenizer.tokens.save_to_file(project.data_path + batch_id + "-tokendata.pickle")
    list_of_records = tokenizer.getRecords()
    
    print len(tokenizer.tokens.token_2_index.keys())
    
    
    project.list_of_records = list_of_records
    
    # dimension of input vectors
    dim = tokenizer.tokens.no_of_tokens

    D = Disambiguator(list_of_records, tokenizer.tokens.index_2_token, tokenizer.tokens.token_2_index, dim)
    project.D = D
    D.project = project
    
    
    # desired dimension (length) of hashes
    hash_dim = 20
    project.log('Hash dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 20
    
    
    # Number of times the hashes are permutated and sorted
    no_of_permutations = 5
    project.log('Number of permutations' , str(no_of_permutations))

    
#     self.D = Disambiguator(self.list_of_vectors, self.tokens.index_2_token, self.tokens.token_2_index, dim, self.batch_id)
    
    
    print 'Analyze...'
    t1 = time.time()
    # compute the hashes
    D.compute_LSH_hash(hash_dim)
    D.save_LSH_hash(batch_id=batch_id)
    D.compute_similarity(B1=B, m=no_of_permutations , sigma1=None)
    
#     print json.dumps(D.dict_already_compared_pairs.keys(),indent=2)
#     quit()
    # D.show_sample_adjacency()

    t2 = time.time()
    print 'Done...'
    print t2 - t1


    
# Set the main pieces of data for the project: list of records (identifiers) and list of records (auxiliary)
#     project.set_list_of_records_identifier(list_of_records_identifier)
#     project.set_list_of_records_auxiliary(list_of_records_auxiliary)
#     
#     # set the list of identifier and auxiliary fields
#     project.set_list_tokenized_fields(list_tokenized_fields)
#     project.set_auxiliary_fields(auxiliary_fields)

    
    print 'Saving adjacency matrix to file...'
    project.save_graph_to_file(list_of_nodes=[])
    project.save_graph_to_file_json(list_of_nodes=[])
    print 'Done...'
    # project.D.imshow_adjacency_matrix(r=(0, record_no))
    
    print 'Printing list of identifiers and text of adjacency matrix to file...'
    project.save_data(with_tokens = False)
    print 'Done...'
    
    time2 = time.time()
    print time2 - time1

    print project.__dict__.keys()
    project.saveSettings()

#     for record in list_of_records:
#         print record.vector





class Project:
    def __init__(self, batch_id):
        self.batch_id = batch_id
        self.data_path = os.path.expanduser('~/data/FEC/')
        self.logfilename = '../records/' + str(self.batch_id) + '.record'
        self.logfile = open(self.logfilename, 'w', 0)
        self.messages = []
        self.list_tokenized_fields = []
        self.list_auxiliary_fields = []
        
    def saveSettings(self):
        settings = {}
        for item in self.__dict__:
            if item not in ['list_of_records', "D", "tokenizer"]:
#             if True:
                try:
                    tmp = json.dumps(self.__dict__[item])
                    settings[item] = self.__dict__[item]
                except TypeError:
                    print "WWARNING: ", item, " not serializable. Skiping." 
        f = open(self.data_path + self.batch_id + '-settings.json', 'w')
        f.write(json.dumps(settings, indent=4))
        f.close()
    
    def log(self, key, value):
        self.messages.append((key, value))
        self.logfile.write("%s : %s\n" % (key, value))  # write without buffering
#         self.logfile.close()
#         self.logfile = open(self.logfilename, 'w')
    
    def setBatchId(self, batch_id):
        self.batch_id = batch_id
        self.log('Batch ID' , self.batch_id)

        
      

    def save_graph_to_file_json(self, filename=None, list_of_nodes=[]):
        if not filename: filename = self.data_path + self.batch_id + '-adjacency.json'
        # save adjacency matrix to file
        f = open(filename, 'w') 
        if  not self.D.adjacency: return 
        if not list_of_nodes: list_of_nodes = range(len(self.list_of_records))
        nmin = list_of_nodes[0]
        list_of_links = []
        for node1 in list_of_nodes:
            for node2 in self.D.adjacency[node1]:
                list_of_links.append((node1 - nmin, node2 - nmin))
        f.write(json.dumps(list_of_links))
        f.close()

    def save_graph_to_file(self, filename=None, list_of_nodes=[]):
        if not filename: filename = self.data_path + self.batch_id + '-adjacency.edges'
        # save adjacency matrix to file
        f = open(filename, 'w') 
        if  not self.D.adjacency: return 
        if not list_of_nodes: list_of_nodes = range(len(self.list_of_records))
        nmin = list_of_nodes[0]
        for node1 in list_of_nodes:
            for node2 in self.D.adjacency[node1]:
                f.write(str(node1 - nmin) + ' ' + str(node2 - nmin) + "\n")
        f.close()
        
    def set_list_of_records_auxiliary(self, tmp_list):
        ''' This functions sets the list of auxiliary records associated with the items in list_of_records_identifier'''
        self.list_of_records_auxiliary = tmp_list
    def set_list_of_records_identifier(self, list_of_records_identifier):
        ''' This functions sets the main list of strings on which the similarity analysis is performed'''
        self.list_of_records_identifier = list_of_records_identifier
    
    
    def save_data(self, r=[], verbose=False,with_tokens = False):
            ''' This function does three things:
                1- saves a full description of the nodes with all attributes in json format to a file <batch_id>-list_of_nodes.json
                   This file, together with the <batch-id>-adjacency.txt file provides all the information about the graph and its
                   node attributes.
                2- saves a formatted text representation of the adjacency matrix with identifier information
                3- saves a formatted text representation of the adjacency matrix with auxiliary field information.
            '''
            
            filename1 = self.data_path + self.batch_id + '-adj_text_identifiers.json'
            filename2 = self.data_path + self.batch_id + '-adj_text_auxiliary.json'
            filename3 = self.data_path + self.batch_id + '-list_of_nodes.json'
            if self.D and self.D.adjacency:
    #             separator = '----------------------------------------------------------------------------------------------------------------------'
                separator = '______________________________________________________________________________________________________________________'
                pp = pprint.PrettyPrinter(indent=4)
#                 pp.pprint(self.D.adjacency)
    
                n = len(self.list_of_records)
                if r:
                    save_range = range(max(0, r[0]), min(n, r[1]))
                else: 
                    save_range = range(len(self.list_of_records))
    
                file1 = open(filename1, 'w')
                file2 = open(filename2, 'w')
                file3 = open(filename3, 'w')
                dict_all3 = {}
                
                list_tokens = []
                for i in save_range:
                    list_tokens.append(self.tokenizer._get_tokens(self.list_of_records[i], self.list_tokenized_fields))
                    
                for i in save_range:

                    tmp_tokens = list_tokens[i]
                    tokens_str = [str(x) for x in tmp_tokens]
                    tokens = {x[0]:x[1] for x in tmp_tokens} 
                    
                    record_as_list_tokenized = [self.list_of_records[i][field] for field in sorted(self.list_tokenized_fields)]
                    record_as_list_auxiliary = [self.list_of_records[i][field] for field in sorted(self.list_auxiliary_fields)]

#                     dict_all3[i] = {'ident':record_as_list_tokenized, 'aux':record_as_list_auxiliary, 'ident_tokens':tokens}
                    dict_all3[i] = {'data':[self.list_of_records[i][field] for field in self.all_fields],
                                             'ident_tokens':tokens}
                    

                    if with_tokens:
                        s1 = "%d %s        %s\n" % (i, record_as_list_tokenized , '|'.join(tokens_str))
                    else:
                        s1 = "%d %s\n" % (i, record_as_list_tokenized)

                    s2 = "%d %s \n" % (i, record_as_list_auxiliary)
                    file1.write(separator + '\n' + s1)   
                    file2.write(separator + '\n' + s2)
                    for j in sorted(self.D.adjacency[i], key=lambda k:self.list_of_records[k]['TRANSACTION_DT']):
#                         record_as_list_tokenized__2 = [self.list_of_records[j][field] for field in sorted(self.list_of_records[j].keys())]
                        record_as_list_tokenized__2 = [self.list_of_records[j][field] for field in sorted(self.list_tokenized_fields)]
                        record_as_list_auxiliary__2 = [self.list_of_records[j][field] for field in sorted(self.list_auxiliary_fields)]
                        
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
    #                     tmp_neighbor1 = [j,self.list_of_records_identifier[j],tokens]
    #                     tmp_neighbor2 = [j,self.list_of_records_auxiliary[j]]
    #                     list1.append(tmp_neighbor1)
    #                     list2.append(tmp_neighbor2)
    #                 dict_all1[i]={}
    #                 dict_all2[i]={}
    #                 dict_all1[i]['neighbors'] = list1
    #                 dict_all1[i]['node'] = tmp_record1 
    #                 dict_all2[i]['neighbors'] = list2
    #                 dict_all2[i]['node'] = tmp_record2 
                file3.write(json.dumps(dict_all3))    
                
                file1.close()
                file2.close()
                
            



if __name__ == '__main__':
    main()
