'''
Created on Jul 1, 2014

@author: navid
'''
'''
TODO:
    The address tokenizer fails in the case of PO BOX addresses.
'''

import json
import os
import pickle
import pprint
import re

from address import AddressParser
from nltk.util import ngrams


class Tokenizer():
    ''' This class receives a list of records (perhaps retrieved from a MySQL query). The fields are divided between "identifier" fields
        and "auxiliary" fields.
        The class tokenizes all the identifier fields of all records, and for each record computes a vector that encapsulates this information.
        The set of these vectors is then sent to a Disambiguator instance which computes a similarity graph between the nodes(records) based
        on their vectors.

        QUESTION: How do we use the auxiliary fields as well?'''
   
        
        
    def __init__(self):
        self.list_of_records = []
        self.tokens = TokenData()
        
        # This is the main data passed to the Disambiguator object.
        # A list where each element is a vector: a dictionary {token_index: (0 or 1)} showing which tokens exist in the given record.
        # A token index is an integer assigned to each unique tuple (token_identifier, string) where a token identifier is an integer
        # indicating the type of the token (Last_NAME? FIRST_NAME? CONTRIBUTOR_ZIP? etc?)
        
        self.list_of_records_identifier = []
        
        # These functions update the record in place by adding new items to the Record's dictionary for each attribute
        # and inserting into it a normalized version of the attribute.
        self.normalize_functions = {'NAME':self._normalize_NAME,
                                    'LAST_NAME':self._normalize_LAST_NAME,
                                    'FIRST_NAME':self._normalize_FIRST_NAME,
                                    'CONTRIBUTOR_ZIP':self._normalize_ZIP,
                                    'CONTRIBUTOR_STREET_1':self._normalize_STREET}
        
        # I think I won't need to use these now, since names are already split up into their parts
        self.tokenize_functions = {'NAME':self._get_tokens_NAME,
                                   'LAST_NAME':self._get_tokens_LAST_NAME,
                                   'FIRST_NAME':self._get_tokens_FIRST_NAME,
                                   'CONTRIBUTOR_ZIP':self._get_tokens_ZIP,
                                   'CONTRIBUTOR_STREET_1':self._get_tokens_STREET}
       
#         # Used when NAME is retrieved from MySQL query, not FIRST_NAME and LAST_NAME
#         self.token_identifiers = {'NAME':[1, 2, 3],
#                                 'CONTRIBUTOR_ZIP':[4],
#                                 'CONTRIBUTOR_STREET_1':[5]}
        self.token_identifiers = {'NAME':[1, 2, 3],
                                   'LAST_NAME':[1],
                                   'FIRST_NAME':[2],
                                   'CONTRIBUTOR_ZIP':[4],
                                   'CONTRIBUTOR_STREET_1':[5]}
        self.ap = AddressParser()
#         self.query = ''
        self.data_path = os.path.expanduser('~/data/FEC/')

     
   
    def save_data(self, r=[], verbose=False):
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
            pp.pprint(self.D.adjacency)

            n = len(self.list_of_records)
            if r:
                save_range = range(max(0, r[0]), min(n, r[1]))
            else: 
                save_range = range(len(self.list_of_records_identifier))

            file1 = open(filename1, 'w')
            file2 = open(filename2, 'w')
            file3 = open(filename3, 'w')
            dict_all3 = {}
            for i in save_range:
                tmp_tokens = self._get_tokens(self.list_of_records_identifier[i])
                tokens_str = [str(x) for x in tmp_tokens]
                tokens = {x[0]:x[1] for x in tmp_tokens} 
                tmp_record1 = [i, self.list_of_records_identifier[i], tokens]
                tmp_record2 = [i, self.list_of_records_auxiliary[i]]
                dict_all3[i] = {'ident':self.list_of_records_identifier[i], 'aux':self.list_of_records_auxiliary[i], 'ident_tokens':tokens}

                s1 = "%d %s        %s\n" % (i, self.list_of_records_identifier[i]  , '|'.join(tokens_str))
                s2 = "%d %s \n" % (i, self.list_of_records_auxiliary[i])
                file1.write(separator + '\n' + s1)   
                file2.write(separator + '\n' + s2)
                for j in self.D.adjacency[i]:
                    tmp_tokens = [str(x) for x in self._get_tokens(self.list_of_records_identifier[j])]
                    tokens_str = [str(x) for x in tmp_tokens]
                    tokens = {x[0]:x[1] for x in tmp_tokens} 
                    s1 = "    %d %s        %s\n" % (j, self.list_of_records_identifier[j]  , '|'.join(tokens_str))
                    s2 = "    %d %s \n" % (j, self.list_of_records_auxiliary[j])
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
            
            
   
   
    # Void. updates the record
    def _normalize_NAME(self, record):
        ''' this normalizer is applied to the whole name, that is, when first/middle/last name and titles are all mixed into one string.'''
        # remove all numerals
        s = record['NAME']
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
        
        # Compactify multiple whitespaces
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

#         tokens = []
#         if middle_name==[]: middle_name=''
        tmp = list(self.token_identifiers)
        
        for s in last_name_list:
            record['N_last_name'].append(s)
            
        for s in first_name_list:
            record['N_first_name'].append(s)
            
        # middle_name is set up to be a string at this point
        if len(middle_name) > 0:
            record['N_middle_name'].append(s)
        
     
    ''' Not implemented since we mostly use the NAME fields of the FEC database instead '''
    def _normalize_LAST_NAME(self, record):
        pass
    
    ''' Not implemented since we mostly use the NAME fields of the FEC database instead '''
    def _normalize_FIRST_NAME(self, record):
        pass
    
    # Void. updates the record
    def _normalize_ZIP(self, record):
        s = record['CONTRIBUTOR_ZIP']
        record['N_zipcode'].append(s if len(s) < 5 else s[0:5]) 
    
    # Void. updates the record
    def _normalize_STREET(self, record):
        s = record['CONTRIBUTOR_STREET_1']
        try:
            address = self.ap.parse_address(s)
        
            tmp = [address.house_number, address.street_prefix, address.street, address.street_suffix]
            tmp = [x  for x in tmp if x is not None]

            address_new = ' '.join(tmp)
#             print "ADDRESS parsed properly=================================================================="
            record["N_address"] = address_new
        except:
#             print 'error'
#             print "ADDRESS FAILED=================================================================="
            record["N_address"] = s

   
   
   
   
  
   
   
   
   
   
   
        
    ''' This function tokenizes a list of selected fields for the given record'''        
    def _get_tokens(self, record, list_fields):
        tokens = []
        # Iterate through all specified fields in the retrieved record
        for field in list_fields:
            # Get the field 
            try:
                s = record[field]
            except KeyError:
                print "record does not contain field %s" % field
                continue
            # Tokenize only those fields that have a tokenizer function specified in self.tokenize_functions
            if field in self.tokenize_functions:
                
                # For each field, call the appropriate normalize function
                # NOTE: has to be done before calling tokenize functions
                self.normalize_functions[field](record)

                # For each field type, call the appropriate tokenize function'''
                tokens += self.tokenize_functions[field](record)
#         print tokens
        return tokens
    
    #===========================================================================
    # 
    #===========================================================================

    def _get_tokens_FIRST_NAME(self, record):
        name = record['N_first_name']
        ''' Some first names are contaminated with middle names. clean them up.'''
        identifier = self.token_identifiers['FIRST_NAME'][0]
        
        
        return [(identifier, s) for s in name]
    
    #===========================================================================
    # 
    #===========================================================================
    def _get_tokens_LAST_NAME(self, record):
        lastname = record['N_last_name']
        identifier = self.token_identifiers['LAST_NAME'][0]
        return [(identifier, s) for s in lastname]        
    
    #===========================================================================
    # 
    #===========================================================================
    def _get_tokens_ZIP(self, record):
        zipcode = record['N_zipcode']
        identifier = self.token_identifiers['CONTRIBUTOR_ZIP'][0]
        return [(identifier, s) for s in zipcode]
    
    #===========================================================================
    # 
    #===========================================================================
    def _get_tokens_STREET(self, record):
        address = record['N_address']
        identifier = self.token_identifiers['CONTRIBUTOR_STREET_1'][0]
        return [(identifier, s) for s in address]

        
    #===========================================================================
    # 
    #===========================================================================
    def _get_tokens_NAME(self, record):
        lastname = record['N_lastname']
        firstname = record['N_first_name']
        middlename = record['N_middle_name']
        
        tokens = []
        
        # last name
        identifier = self.token_identifiers['NAME'][0]
        tokens += [(identifier, s) for s in lastname]
        
        # first name
        identifier = self.token_identifiers['NAME'][1]
        tokens += [(identifier, s) for s in firstname]
        
        # middle name
        identifier = self.token_identifiers['NAME'][2]
        if len(middlename) > 0:
            tokens += [(identifier, middlename[0])]
    
        return tokens    
        
                
    
    def tokenize(self):
        if not self.list_tokenized_fields:
            raise Exception("ERROR: Specify the fields to be tokenized.")
        
        for record in self.list_of_records:
            
            # s_plit is a list of tuples: [(1,'sdfsadf'),(2,'ewre'),...]     
            s_split = self._get_tokens(record, self.list_tokenized_fields)
            # A dictionary {token_index: (0 or 1)} showing which tokens exist in the given record
            record.vector = {}
            
            
            # Here, each token is a tuple (token_identifier(name? address?, etc?), string)
            for token in s_split:
                if token in self.tokens.token_2_index:
                    self.tokens.token_counts[token] += 1
                else:
                    self.tokens.token_2_index[token] = self.tokens.no_of_tokens
                    self.tokens.index_2_token[self.tokens.no_of_tokens] = token
                    self.tokens.token_counts[token] = 1
                    self.tokens.no_of_tokens += 1
                record.vector[self.tokens.token_2_index[token]] = 1
                
        self.all_token_sorted = sorted(self.tokens.token_counts, key=self.tokens.token_counts.get, reverse=0)
#         for token in self.all_token_sorted:
#             print token, self.tokens.token_counts[token],'---------'
        print "Total number of tokens identified: ", len(self.tokens.token_counts)
#         pp.pprint(self.list_of_records_identifier)
#         quit()

        # set "self" as the Tokenizer object associated with each record
        for record in self.list_of_records:
            record.tokendata = self.tokens


    
  
    def setTokenizedFields(self, list_tokenized_fields):
        ''' List of MySQL table "identifier" fields retrieved using the original MySQL query'''
        self.list_tokenized_fields = list_tokenized_fields
    
  


          
    def setRecords(self, list_of_records):
            self.list_of_records = list_of_records
        
    def getRecords(self):
        return self.list_of_records   

#===========================================================================
#     Class containing all token data
#===========================================================================



'''
Subclass of Tokenizer that implements different tokenizer functions for FIRST_NAME,LAST_NAME,etc
'''
class TokenizerNgram(Tokenizer):
    
    def __init__(self):
        Tokenizer.__init__(self)
        
        # Features are bigrams
        self.ngram_n = 2
       
    ''' split string into tokens and then extract all ngrams in all tokens '''
    def ngrams(self, sentence, n):
        list_ngrams = []
        tokens = sentence.split()
        for token in tokens:
            myngrams = ngrams(token, n)
            for grams in myngrams:
                list_ngrams.append("".join(grams)) 
        return list_ngrams
    
    # Override: here, tokenz are ngrams
    def _get_tokens_NAME(self, record):
        lastname = record['N_last_name']
        firstname = record['N_first_name']
        middlename = record['N_middle_name']
        
        tokens = []
        
        # last name
        identifier = self.token_identifiers['NAME'][0]
        tokens += [(identifier, s) for word in lastname for s in self.ngrams(word, self.ngram_n) ]

        
        # first name
        identifier = self.token_identifiers['NAME'][1]
        tokens += [(identifier, s) for word in firstname for s in self.ngrams(word, self.ngram_n) ]
        # middle name
        identifier = self.token_identifiers['NAME'][2]
        if len(middlename) > 0:
            tokens += [(identifier, middlename[0])]
    
        return tokens    
        
                
    
    # Override: here, tokens are ngrams
    def _get_tokens_FIRST_NAME(self, record):
        firstname = record['N_first_name']
        identifier = self.token_identifiers['NAME'][1]
        tokens = [(identifier, s) for word in firstname for myngram in self.ngrams(word, self.ngram_n) for s in myngram]
        return  tokens
    
    
    # Override: here, tokens are ngrams
    def _get_tokens_LAST_NAME(self, record):
        lastname = record['N_last_name']
        identifier = self.token_identifiers['NAME'][0]
        tokens = [(identifier, s) for word in lastname for myngram in self.ngrams(word, self.ngram_n) for s in myngram]
        return  tokens
    
   
    def _get_tokens_ZIP(self, record):
        return Tokenizer._get_tokens_ZIP(self, record)
    
   
    # override
    def _get_tokens_STREET(self, record):
        return Tokenizer._get_tokens_STREET(self, record)

        


'''
This class encapsulates the global token data derived from all
records processed by a Tokenizer instance.
'''
class TokenData():
    def __init__(self):
        self.token_2_index = {}
        self.index_2_token = {}
        self.no_of_tokens = 0
        self.token_counts = {}
        # Load the name variants file
        self.dict_name_variants = json.load(open('../data/name-variants.json'))
    def save_to_file(self, filename):
        pickler = pickle.Pickler(open(filename, 'w'))
        pickler.dump(self)

   
