#! /usr/bin/python

# Main branch

import numpy as np
import pylab as pl
import csv
import MySQLdb as mdb
import re
# import nltk
from Disambiguator import *
import pprint
import time
from address import AddressParser
import json
import datetime
import sys

# establish and return a connection to the MySql database server
def db_connect():
    con = None
    con = mdb.connect(host='localhost',  # hostname
                       user='navid',  # username                
                       passwd='YOURMYSQLPASSWORD',  # password
                       db='FEC',  # database 
                       use_unicode=True,
                       charset="utf8"
    )
    if con == None: print("Error connecting to MySql server")
    return con  

def MySQL_query(query):
    con = db_connect()
    cur = con.cursor()
    # cur.execute("select NAME,ZIP_CODE,EMPLOYER,TRANSACTION_DT from newyork order by NAME limit 1000 ;")
    cur.execute(query)
    return  cur.fetchall()
    
    


class FEC_analyst():
    
    def __init__(self, batch_id):
        # con = db_connect()
        # cur = con.cursor()
        # cur.execute("select NAME,ZIP_CODE,EMPLOYER,TRANSACTION_DT from newyork order by NAME limit 1000 ;")
        # cur.execute("select distinct NAME from newyork  order by NAME limit 100;")
        # a = cur.fetchall()
        self.batch_id = batch_id
        self.hash_dim = None
        self.D = []  # Disambiguator object
        self.token_counts = {}
        self.token_2_index = {}
        self.index_2_token = {}
        
        self.all_tokens_sorted = []
        self.list_of_vectors = []
        self.no_of_tokens = 0
        self.list_of_identifiers = []
        
        # I think I won't need to use these now, since names are already split up into their parts
        self.tokenize_functions = {'NAME':self.__get_tokens_NAME,
                                   'LAST_NAME':self.__get_tokens_LAST_NAME,
                                   'FIRST_NAME':self.__get_tokens_FIRST_NAME,
                                 'CONTRIBUTOR_ZIP':self.__get_tokens_ZIP,
                                 'CONTRIBUTOR_STREET_1':self.__get_tokens_STREET}
       
#         # Used when NAME is retrieved from MySQL query, not FIRST_NAME and LAST_NAME
#         self.token_identifiers = {'NAME':[1, 2, 3],
#                                 'CONTRIBUTOR_ZIP':[4],
#                                 'CONTRIBUTOR_STREET_1':[5]}
        self.token_identifiers = {'LAST_NAME':[1],
                                  'FIRST_NAME':[2],
                                'CONTRIBUTOR_ZIP':[4],
                                'CONTRIBUTOR_STREET_1':[5]}
        self.ap = AddressParser()
        self.query = ''
     
   
    def save_data(self, r=[], verbose=False):
        ''' This function does three things:
            1- saves a full description of the nodes with all attributes in json format to a file <batch_id>-list_of_nodes.json
               This file, together with the <batch-id>-index_adjacency.txt file provides all the information about the graph and its
               node attributes.
            2- saves a formatted text representation of the index_adjacency matrix with identifier information
            3- saves a formatted text representation of the index_adjacency matrix with auxilliary field information.
        '''
        
        filename1 = '../results/' + self.batch_id + '-adj_text_identifiers.json'
        filename2 = '../results/' + self.batch_id + '-adj_text_auxilliary.json'
        filename3 = '../results/' + self.batch_id + '-list_of_nodes.json'
        if self.D and self.D.index_adjacency:
#             separator = '----------------------------------------------------------------------------------------------------------------------'
            separator = '______________________________________________________________________________________________________________________'
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.D.index_adjacency)

            n = len(self.list_of_identifiers)
            if r:
                save_range = range(max(0,r[0]), min(n,r[1]))
            else: 
                save_range = range(len(self.list_of_identifiers))

            file1 = open(filename1, 'w')
            file2 = open(filename2, 'w')
            file3 = open(filename3, 'w')
            dict_all3 = {}
            for i in save_range:
                tmp_tokens = self.__get_tokens(self.list_of_identifiers[i])
                tokens_str = [str(x) for x in tmp_tokens]
                tokens = {x[0]:x[1] for x in tmp_tokens} 
                tmp_record1 = [i, self.list_of_identifiers[i], tokens]
                tmp_record2 = [i, self.list_of_auxilliary_records[i]]
                dict_all3[i] = {'ident':self.list_of_identifiers[i], 'aux':self.list_of_auxilliary_records[i], 'ident_tokens':tokens}

                s1 = "%d %s        %s\n" % (i, self.list_of_identifiers[i]  , '|'.join(tokens_str))
                s2 = "%d %s \n" % (i, self.list_of_auxilliary_records[i])
                file1.write(separator+'\n'+s1)   
                file2.write(separator+'\n'+s2)
                for j in self.D.index_adjacency[i]:
                    tmp_tokens = [str(x) for x in self.__get_tokens(self.list_of_identifiers[j])]
                    tokens_str = [str(x) for x in tmp_tokens]
                    tokens = {x[0]:x[1] for x in tmp_tokens} 
                    s1 = "    %d %s        %s\n" % (j, self.list_of_identifiers[j]  , '|'.join(tokens_str))
                    s2 = "    %d %s \n" % (j, self.list_of_auxilliary_records[j])
                    file1.write(s1)   
                    file2.write(s2)    
#                     tmp_neighbor1 = [j,self.list_of_identifiers[j],tokens]
#                     tmp_neighbor2 = [j,self.list_of_auxilliary_records[j]]
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
            
        
    def __get_tokens(self, record):
        ''' This function tokenizes a record from the MySQL query results stored in self.list_of_fields.
            A record can have an arbitrary number of fields. For our purposes, the main one is NAME, but
            there are also address-related fields. Each one is tokenized and the tokens are added to a
            "vector" as a (field/subfield-identifier, token) tuple.'''
        tokens = []
        for field, field_index in zip(self.identifier_fields, range(len(self.identifier_fields))):
            s = record[field_index]
            if field in self.tokenize_functions:
                ''' For each field type, call the appropriate tokenize function'''
                tokens += self.tokenize_functions[field](s)
#         print tokens
        return tokens
    
                
    def __get_tokens_FIRST_NAME(self, name):
        ''' Some first names are contaminated with middle names. clean them up.'''
        identifier = self.token_identifiers['FIRST_NAME'][0]
        s = re.sub(r'\b\w\b\.?',' ',name)
        parts = re.split(r'\s+',s)
        sizes = [len(x) for x in parts]
        largest_index = sizes.index(max(sizes))
        s = parts[largest_index]
        return [(identifier, s)]
    def __get_tokens_LAST_NAME(self, s):
        identifier = self.token_identifiers['LAST_NAME'][0]
        return [(identifier, s)]        
            
    def __get_tokens_ZIP(self, s):
        identifier = self.token_identifiers['CONTRIBUTOR_ZIP'][0]
        zipcode = s if len(s) < 5 else s[0:5] 
        return [(identifier, zipcode )]
    def __get_tokens_STREET(self, s):
        try:
            address = self.ap.parse_address(s)
        
            tmp = [address.house_number, address.street_prefix, address.street, address.street_suffix]
            tmp = [x  for x in tmp if x is not None]
#             print s
#             print tmp
#             print '__________________________'

            address_new = ' '.join(tmp)
            identifier = self.token_identifiers['CONTRIBUTOR_STREET_1'][0]
            return [(identifier, address_new)]
        except:
#             print 'error'
            identifier = self.token_identifiers['CONTRIBUTOR_STREET_1'][0]
            return [(identifier, s)]
    def __get_tokens_NAME(self, s):
        ''' this tokenizer is applied to the whole name, that is, when first/middle/last name and titles are all mixed into one string.'''
        # remove all numerals
        s1 = re.sub(r'\.|[0-9]+', '', s)
        
        # List of all suffixes found then remove them
        suffix_list = re.findall(r'\bESQ\b|\bENG\b|\bINC\b|\bLLC\b|\bLLP\b|\bMRS\b|\bPHD\b|\bSEN\b', s)
        suffix_list += re.findall(r'\bDR\b|\bII\b|\bIII\b|\bIV\b|\bJR\b|\bMD\b|\bMR\b|\bMS\b|\bSR\b', s)
        s1 = re.sub(r'(\bESQ\b)|(\bENG\b)|(\bINC\b)|(\bLLC\b)|(\bLLP\b)|(\bMRS\b)|(\bPHD\b)|(\bSEN\b)', '', s1)
        s1 = re.sub(r'(\bDR\b)|(\bII\b)|(\bIII\b)|(\bIV\b)|(\bJR\b)|(\bMD\b)|(\bMR\b)|(\bMS\b)|(\bSR\b)', '', s1)
     
        s1 = re.sub(r'\.', '', s1)
                
        # Find the first single-letter token and save it as middle name, then remove it 
        middle_name_single = re.findall(r'\b\w\b', s1)
        if middle_name_single:
            middle_name_single = middle_name_single[0]
            s1 = re.sub(r'\b%s\b' % middle_name_single, '', s1)
        else:
            middle_name_single = ''    
        
        # Replace all word separators with spaces
#         s1 = re.sub(r'[\-\/;\+()]*', ' ', s1)
        
        # Compactify multiply whitespaces
        s1 = re.sub(r'\s+', ' ', s1)
        
#         print s1
        # Extract full last name and tokenize, then remove from s1
        last_name = re.findall(r'^[^,]*(?=\,)', s1)
        
        if not last_name:
            last_name_list = []
            first_name_list = []
            middle_name = []
            
        else:
            last_name = last_name[0]        
            s1 = s1.replace(last_name, '')
            # Remove surrounding whitespace from last name
            last_name = re.sub(r'^\s+|\s+$', '', last_name)
            last_name_list = re.split(r' ', last_name)
            
#             print s1
            # Extract first name
            first_name = re.findall(r'[^,]+', s1)
            if first_name:
                first_name = first_name[0]
                # Remove surrounding whitespace from last name
                first_name = re.sub(r'^\s+|\s+$', '', first_name)
                first_name_list = re.split(r' ', first_name)
            else:
                first_name_list = []
            
            if len(first_name_list) > 1 and not middle_name_single:
                middle_name = first_name_list[-1]
                first_name_list.remove(middle_name)
            else: 
                middle_name = middle_name_single

        ''' Print NAME tokens'''
#         print s
#         print "    First name:--------- ", first_name_list
#         print "    Las tname:--------- ", last_name_list
#         print "    Middle name:--------- ", middle_name
#         print "    Suffixes:--------- ", suffix_list

        tokens = []
#         if middle_name==[]: middle_name=''
        tmp = list(self.token_identifiers)
        
        identifier = self.token_identifiers['NAME'][0]
        for s in last_name_list:
            tokens.append((identifier, s))
        identifier = self.token_identifiers['NAME'][1]
        for s in first_name_list:
            tokens.append((identifier, s))
#         for s in suffix_list:
#             tokens.append((4,s))
            
        # middle_name is set up to be a string at this point
        identifier = self.token_identifiers['NAME'][2]
        if len(middle_name) > 0:
            tokens.append((identifier, middle_name[0]))
        
        return tokens    
        
                

    
    def __get_tokens_old(self, s):
        ''' This function returns a tuple (index,word) where index is in {1,2,3} and indicates whether the word
            was part of the first name, last name of middle name. word is the actual token.'''
        # Remove suffied such as MR, MS, MRS
        s1 = re.sub(r'( MRS\b)|( MR\b)|( MS\b)|( DR\b)|\.', '', s)
        s1 = re.sub(r'[\-\/;+()]*', ' ', s1)
        s1 = re.sub(r'\s+', ' ', s1)
        
        # Find and remove last name
        # Remove middle name initials
#         print s
        s1 = re.sub(r' [a-zA-Z]$| [a-zA-Z][ .]', ' ', s1)
#         print s,'\n\n'
        s1 = re.sub(r' $', '', s1)
       
        # NLTK tokenize
        tokens = re.split(r' ', s1)
        
        if len(tokens) > 1:
            return tokens
        else: 
            s1 = re.sub(r'\,|\.', '', s)
            s1 = re.sub(r'[\-\/;+()]*#', ' ', s1)
            s1 = re.sub(r'\s+', ' ', s1)
            # DON'T! Remove middle name initials
#             s1 = re.sub(r' [a-zA-Z]$| [a-zA-Z][ .]',' ',s1)
            s1 = re.sub(r' $', '', s1)
        
        tokens = re.split(r' ', s1)
        if len(tokens) > 1:
            return tokens    
        else: 
            tokens.append('!!ERROR!!') 
            return tokens
 
#         return nltk.word_tokenize(s)
        
         
    
    def tokenize(self):
        for i in range(len(self.list_of_identifiers)):           
            s = self.list_of_identifiers[i]
            
            # I believe this is unneccesary 
            s = s[0:len(self.identifier_fields)]
            # print s
            # s_plit is a list of tuples: [(1,'sdfsadf'),(2,'ewre'),...]     
            s_split = self.__get_tokens(s)
#             print s_split
            
        #     print '----------------------'
            vec = {}
            for token in s_split:
                if token in self.token_2_index:
                    self.token_counts[token] += 1
                else:
                    self.token_2_index[token] = self.no_of_tokens
                    self.index_2_token[self.no_of_tokens] = token
                    self.token_counts[token] = 1
                    self.no_of_tokens += 1
                vec[self.token_2_index[token]] = 1
            self.list_of_vectors.append(vec)
                
        self.token_frequencies = sorted(self.token_counts.values(), reverse=1)
        
        self.all_token_sorted = sorted(self.token_counts, key=self.token_counts.get, reverse=0)
#         for token in self.all_token_sorted:
#             print token, self.token_counts[token],'---------'

        print "Total number of tokens identified: ", len(self.token_counts)
#         pp.pprint(self.list_of_identifiers)
#         quit()



    
    def set_query(self, query):
        ''' The MySQL query used'''
        self.query = query
    
    def set_query_fields(self, query_fields):
        ''' List of ALL MySQL table fields retrieved using the original MySQL query'''
        self.query_fields = query_fields
    def set_identifier_fields(self, identifier_fields):
        ''' List of MySQL table "identifier" fields retrieved using the original MySQL query'''
        self.identifier_fields = identifier_fields
    def set_auxilliary_fields(self, auxilliary_fields):
        ''' List of MySQL table "auxilliary" fields retrieved using the original MySQL query'''
        self.auxilliary_fields = auxilliary_fields
        
    def save_job_record(self):
        f = open('../records/' + str(self.batch_id) + '.record', 'w')
        f.write('Time: ' + datetime.datetime.now().isoformat() + '\n')
        f.write('Batch ID: ' + self.batch_id + '\n')
        f.write('MySQL query: ' + self.query + '\n')
        f.write('Identifier fields: ' + ','.join(self.identifier_fields) + '\n')
        f.write('Auxilliary fields: ' + ','.join(self.auxilliary_fields) + '\n')
        f.close()
        

    def save_graph_to_file_json(self, filename=None, list_of_nodes=[]):
        if not filename: filename = '../results/' + self.batch_id + '-index_adjacency.json'
        # save index_adjacency matrix to file
        f = open(filename, 'w') 
        if  not self.D.index_adjacency: return 
        if not list_of_nodes: list_of_nodes = range(len(self.list_of_identifiers))
        nmin = list_of_nodes[0]
        list_of_links = []
        for node1 in list_of_nodes:
            for node2 in self.D.index_adjacency[node1]:
                list_of_links.append((node1 - nmin,node2 - nmin))
        f.write(json.dumps(list_of_links))
        f.close()

    def save_graph_to_file(self, filename=None, list_of_nodes=[]):
        if not filename: filename = '../results/' + self.batch_id + '-index_adjacency.txt'
        # save index_adjacency matrix to file
        f = open(filename, 'w') 
        if  not self.D.index_adjacency: return 
        if not list_of_nodes: list_of_nodes = range(len(self.list_of_identifiers))
        nmin = list_of_nodes[0]
        for node1 in list_of_nodes:
            for node2 in self.D.index_adjacency[node1]:
                f.write(str(node1 - nmin) + ' ' + str(node2 - nmin) + "\n")
        f.close()



    def analyze(self, hash_dim=400, sigma=0.2, B=10):
        # Number of random vectors to generate
        # N = 400
        self.hash_dim = hash_dim
        self.tokenize()
        
        # dimension of input vectors
        dim = len(self.token_counts)
        
        # desired dimension (length) of hashes
        # hash_dim = 200
        
        # Number of times the hashes are permutated and sorted
        no_of_permutations = 100
        
        # Hamming distance threshold for index_adjacency 
        # sigma = 0.2
        
        # Number of adjacent hashes to compare
        # B = 10
        
        
        self.D = Disambiguator(self.list_of_vectors, self.index_2_token, self.token_2_index, dim, self.batch_id)
        
        # compute the hashes
        print "Computing the hashes..."
        self.D.compute_LSH_hash(hash_dim)
        print "Hashes computed..."
        
        self.D.save_LSH_hash()
        
        self.D.compute_similarity(B1=B, m=no_of_permutations , sigma1=sigma)
        
            
        self.D.show_sample_adjacency()

    
    def set_list_of_auxilliary_records(self, tmp_list):
        ''' This functions sets the list of auxilliary records associated with the items in list_of_identifiers'''
        self.list_of_auxilliary_records = tmp_list
    def set_list_of_identifiers(self, list_of_identifiers):
        ''' This functions sets the main list of strings on which the similarity analysis is performed'''
        self.list_of_identifiers = list_of_identifiers
    
    

    
        
    
    
            
        
        
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


pp = pprint.PrettyPrinter(indent=4)
# pp.pprint(self.D.index_adjacency)

if len(sys.argv)>1:
    param_state = sys.argv[1]
else:
    param_state = 'newyork'
print "Analyzing data for state: ",param_state 

record_start = 1
record_no = 5000



# batch_id = "[" + str(record_start) + "," + str(record_start + record_no) + "]"
batch_id = get_next_batch_id()

time1 = time. time()
analyst = FEC_analyst(batch_id)


identifier_fields = ['FIRST_NAME','LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
# identifier_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1'] 
auxilliary_fields = ['TRANSACTION_DT', 'EMPLOYER','TRANSACTION_AMT','CITY','CMTE_ID','ENTITY_TP','OCCUPATION']
query_fields = identifier_fields + auxilliary_fields 

index_identifier_fields = [query_fields.index(s) for s in identifier_fields]
index_auxilliary_fields = [query_fields.index(s) for s in auxilliary_fields]    


# Get string list from MySQL query and set it as analyst's list_of_identifiers
# query_result = MySQL_query("select " + ','.join(identifier_fields) + " from newyork_addresses where NAME <> '' order by NAME limit " + str(record_start) + "," + str(record_no) + ";")
query = "select " + ','.join(query_fields) + " from "+param_state+"_addresses order by NAME,TRANSACTION_DT,ZIP_CODE,CMTE_ID limit " + str(record_start) + "," + str(record_no) + ";"

query_result = MySQL_query(query)
tmp_list = []
# for i in range(len(query_result)):
#    tmp_list.append(query_result[i])
    
    # tmp_list.append(query_result[i][0])



# tmp_list = ['Navid, Dianati', 'Navid, Dianati', 'Dianati, Navid A.', 'Navid, Dianati M MR.', 'Navid, Dianati Mr.', 'Navid, Dianati D.', 'Dianati, N. M. MR', 'Navid, Dianati', 'Navid, Dianati', 'Dianati, Navid A.', 'Navid, Dianati M MR.', 'Navid, Dianati Mr.', 'Navid, Dianati D.', 'Dianati, N. M. MR']
tmp_list = [[s.upper() if isinstance(s, basestring) else s.strftime("%Y%m%d") if  isinstance(s, datetime.date) else s  for s in record] for record in query_result]



list_of_identifiers = [[record[index] for index in index_identifier_fields] for record in tmp_list]
list_of_auxilliary_records = [[record[index] for index in index_auxilliary_fields] for record in tmp_list]

# for s in tmp_list:
#     analyst.get_tokens_new(s)
# quit()

# random.shuffle(tmp_list)

analyst.set_list_of_identifiers(list_of_identifiers)
analyst.set_list_of_auxilliary_records(list_of_auxilliary_records)
analyst.set_identifier_fields(identifier_fields)
analyst.set_auxilliary_fields(auxilliary_fields)
analyst.set_query_fields(query_fields)
analyst.set_query(query)
analyst.save_job_record()

print 'Running Analyze...'
t1 = time.time()
analyst.analyze(hash_dim=20, sigma=0.26, B=30)
t2 = time.time()
print 'Done...'
print t2 - t1

print 'Saving index_adjacency matrix to file...'
analyst.save_graph_to_file(list_of_nodes=[])
analyst.save_graph_to_file_json(list_of_nodes=[])

print 'Done...'



# analyst.D.imshow_adjacency_matrix(r=(0, record_no))




    
print 'Printing list of identifiers and text of index_adjacency matrix to file...'
analyst.save_data()
print 'Done...'
# pl.show()

time2 = time.time()

print time2 - time1
