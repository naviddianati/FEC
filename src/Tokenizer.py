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
from nameparser import HumanName

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
        
               
        # These functions update the record in place by adding new items to the Record's dictionary for each attribute
        # and inserting into it a normalized version of the attribute.
        self.normalize_functions = {'NAME':self._normalize_NAME,
                                    'LAST_NAME':self._normalize_LAST_NAME,
                                    'FIRST_NAME':self._normalize_FIRST_NAME,
                                    'CONTRIBUTOR_ZIP':self._normalize_ZIP,
                                    'ZIP_CODE': self._normalize_ZIP,
                                    'CONTRIBUTOR_STREET_1':self._normalize_STREET,
                                    'OCCUPATION': self._normalize_OCCUPATION,
                                    'EMPLOYER': self._normalize_EMPLOYER}
        
        # I think I won't need to use these now, since names are already split up into their parts
        self.tokenize_functions = {'NAME':self._get_tokens_NAME,
                                   'LAST_NAME':self._get_tokens_LAST_NAME,
                                   'FIRST_NAME':self._get_tokens_FIRST_NAME,
                                   'CONTRIBUTOR_ZIP':self._get_tokens_ZIP,
                                   'ZIP_CODE': self._get_tokens_ZIP,
                                   'CONTRIBUTOR_STREET_1':self._get_tokens_STREET,
                                   'OCCUPATION': self._get_tokens_OCCUPATION,
                                   'EMPLOYER': self._get_tokens_EMPLOYER}
       
#         # Used when NAME is retrieved from MySQL query, not FIRST_NAME and LAST_NAME
#         self.token_identifiers = {'NAME':[1, 2, 3],
#                                 'CONTRIBUTOR_ZIP':[4],
#                                 'CONTRIBUTOR_STREET_1':[5]}

        self.token_identifiers = self.tokens.token_identifiers
                            
        self.ap = AddressParser()
#         self.query = ''
        self.data_path = os.path.expanduser('~/data/FEC/')

    # Uses nameparser
    def _normalize_NAME(self, record):
        ''' this normalizer is applied to the whole name, that is, when first/middle/last name and titles are all mixed into one string.'''
        # remove all numerals
        s = record['NAME']
        
        s1 = re.sub(r'\.|[0-9]+', '', s)
        s1 = re.sub(r'(\bESQ\b)|(\bENG\b)|(\bINC\b)|(\bLLC\b)|(\bLLP\b)|(\bMRS\b)|(\bPHD\b)|(\bSEN\b)', '', s1)
        s1 = re.sub(r'(\bDR\b)|(\bII\b)|(\bIII\b)|(\bIV\b)|(\bJR\b)|(\bMD\b)|(\bMR\b)|(\bMS\b)|(\bSR\b)|(\bSGT\b)|(\bDC\b)|(\bREV\b)|(\bFR\b)', '', s1)
        s1 = re.sub(r'\.', '', s1)
                
        name = HumanName(s1)
#         print "%s ----- %s ---- %s ---- %s" % (s, name.last, name.first, name.middle)
       
        
        record['N_last_name'] = name.last.upper()        
            
        record['N_first_name'] = name.first.upper()
            
        record['N_middle_name'] = name.middle.upper()

   
    # Void. updates the record
    # This one doesn't use nameparser. My own version
    def _normalize_NAME_old(self, record):
        ''' this normalizer is applied to the whole name, that is, when first/middle/last name and titles are all mixed into one string.'''
        # remove all numerals
        s = record['NAME']
        
        s1 = re.sub(r'\.|[0-9]+', '', s)
        
        # List of all suffixes found then remove them
#         suffix_list = re.findall(r'\bESQ\b|\bENG\b|\bINC\b|\bLLC\b|\bLLP\b|\bMRS\b|\bPHD\b|\bSEN\b', s)
#         suffix_list += re.findall(r'\bDR\b|\bII\b|\bIII\b|\bIV\b|\bJR\b|\bMD\b|\bMR\b|\bMS\b|\bSR\b', s)
        s1 = re.sub(r'(\bESQ\b)|(\bENG\b)|(\bINC\b)|(\bLLC\b)|(\bLLP\b)|(\bMRS\b)|(\bPHD\b)|(\bSEN\b)', '', s1)
        s1 = re.sub(r'(\bDR\b)|(\bII\b)|(\bIII\b)|(\bIV\b)|(\bJR\b)|(\bMD\b)|(\bMR\b)|(\bMS\b)|(\bSR\b)|(\bSGT\b)|(\bDC\b)', '', s1)
     
        s1 = re.sub(r'\.', '', s1)
                
        name = HumanName(s1)
        print "%s ----- %s ---- %s ---- %s" % (s, name.last, name.first, name.middle)
        return
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
            record['N_middle_name'].append(middle_name[0])
        
     
    ''' Not implemented since we mostly use the NAME fields of the FEC database instead '''
    def _normalize_LAST_NAME(self, record):
        pass
    
    ''' Not implemented since we mostly use the NAME fields of the FEC database instead '''
    def _normalize_FIRST_NAME(self, record):
        pass
    
    # Void. updates the record
    def _normalize_ZIP(self, record):
#         s = record['CONTRIBUTOR_ZIP'] if record['CONTRIBUTOR_ZIP'] else record['ZIP_CODE']
        s = record['ZIP_CODE']
        if not s:
            record['N_zipcode'].append(None)           
        
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
            record["N_address"] = address_new.upper()
        except:
#             print 'error'
#             print "ADDRESS FAILED=================================================================="
            record["N_address"] = s

   
   
   
    # TODO: is it a waste of memory to define record['N_occupation'] ?
    def _normalize_OCCUPATION(self, record):
      
        if record['OCCUPATION']:
            record['N_occupation'] = record['OCCUPATION'].upper()
        else:
            record['N_occupation'] = None
            
         
        
    def _normalize_EMPLOYER(self, record):
        if record['EMPLOYER']:
            record['N_employer'] = record['EMPLOYER'].upper()
        else:
            record['N_employer'] = None
    
       
   
   
   
   
   
        
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
                
                # update dictionary of normalized name frequencies
                self.update_normalized_token_counts(record)

                # For each field type, call the appropriate tokenize function'''
                new_tokens = self.tokenize_functions[field](record)
                if new_tokens: tokens += new_tokens 
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
        if not address:
            return None
        identifier = self.token_identifiers['CONTRIBUTOR_STREET_1'][0]
        return [(identifier, s) for s in address.split()]

        
    #===========================================================================
    # 
    #===========================================================================
    def _get_tokens_NAME(self, record):
        lastname = record['N_last_name']
        firstname = record['N_first_name']
        middlename = record['N_middle_name']
        
        tokens = []
        
        # last name
        identifier = self.token_identifiers['NAME'][0]
        tokens += [(identifier, s) for s in lastname.split(" ")]
        
        # first name
        identifier = self.token_identifiers['NAME'][1]
        tokens += [(identifier, s) for s in firstname.split(" ")]
        
        # middle name
        identifier = self.token_identifiers['NAME'][2]
        if len(middlename) > 0:
            tokens += [(identifier, middlename[0])]
    
        return tokens    
        
                
                
                
               
               
    

    def _get_tokens_OCCUPATION(self, record):
        occupation = record['N_occupation']
        identifier = self.token_identifiers['OCCUPATION'][0]
        tokens = []
        if occupation:
            tokens += [(identifier, s) for s in occupation.split(" ") ]
        return tokens
     
     
    def _get_tokens_EMPLOYER(self, record):
        employer = record['N_employer']
        identifier = self.token_identifiers['EMPLOYER'][0]
        tokens = []
        
        if employer:
            tokens += [(identifier, s) for s in employer.split(" ") ]
        return tokens
     
    
                   
                


    ''' updates self.tokens.normalized_token_counts by tracking/updating the frequency of the
        normalized tokens belonging to the given record.
    ''' 
    def update_normalized_token_counts(self, record):
        last_name = " ".join(record['N_last_name'])
        first_name = " ".join(record['N_first_name'])
        
        # TODO: check if first names and last names are ever empty 
        
        # Last name
        try:
            token_identifier = self.token_identifiers['LAST_NAME'][0]
            self.tokens.normalized_token_counts[(token_identifier, last_name)] += 1
        except KeyError:
            self.tokens.normalized_token_counts[(token_identifier, last_name)] = 1
            
        # First name
        try:
            token_identifier = self.token_identifiers['FIRST_NAME'][0]
            self.tokens.normalized_token_counts[(token_identifier, first_name)] += 1
        except KeyError:
            self.tokens.normalized_token_counts[(token_identifier, first_name)] = 1
            
            
        
        
    def tokenize(self):
        if not self.list_tokenized_fields:
            raise Exception("ERROR: Specify the fields to be tokenized.")
        
        for record in self.list_of_records:
            
            # s_plit is a list of tuples: [(1,'sdfsadf'),(2,'ewre'),...]     
            s_split = self._get_tokens(record, self.list_tokenized_fields)
            # A dictionary {token_index: (0 or 1)} showing which tokens exist in the given record
            record.vector = {}
            
            self.update_normalized_token_counts(record)
            
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
        
        # NOTE: with the new name normalizer, each of the following are a single string!
        
        lastname = record['N_last_name']
        firstname = record['N_first_name']
        middlename = record['N_middle_name']
        
        # Get the "word tokens" and then add ngrams below
        tokens = Tokenizer._get_tokens_NAME(self, record)
        
        # last name
        identifier = self.token_identifiers['NAME'][0]
        
#         tokens += [(identifier, s) for word in lastname for s in self.ngrams(word, self.ngram_n) ]
        tokens += [(identifier, s) for  s in self.ngrams(lastname, self.ngram_n) ]

        
        # first name
        identifier = self.token_identifiers['NAME'][1]
#         tokens += [(identifier, s) for word in firstname for s in self.ngrams(word, self.ngram_n) ]
        tokens += [(identifier, s) for  s in self.ngrams(firstname, self.ngram_n) ]
        
        
        # No need anymore. The superclass method takes care of the middle name        
        # middle name
        # identifier = self.token_identifiers['NAME'][2]
        # if len(middlename) > 0:
        #    tokens += [(identifier, middlename[0])]
    
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

    # If the frequency of a name token is less than this, it is considered rare 
    # and maybe considered a misspelling depending on its edit distance from a similar name 
    RARE_FREQUENCY = 2
    
    
    def __init__(self):
        self.token_2_index = {}
        self.index_2_token = {}
        self.no_of_tokens = 0
        self.token_counts = {}
        
        # A dictionary {(token_id:normalized_token):frequency} showing the 
        # frequency of each normalized token
        self.normalized_token_counts = {}
        # Load the name variants file
        self.dict_name_variants = json.load(open('../data/name-variants.json'))
        self.set_all_names = set(json.load(open('../data/all-names.json')))
        
        self.token_identifiers = {'NAME':[1, 2, 3],
                                   'LAST_NAME':[1],
                                   'FIRST_NAME':[2],
                                   'CONTRIBUTOR_ZIP':[4],
                                   'ZIP_CODE':[4],
                                   'CONTRIBUTOR_STREET_1':[5],
                                   'OCCUPATION':[6],
                                   'EMPLOYER': [7]}
    
    def save_to_file(self, filename):
        pickler = pickle.Pickler(open(filename, 'w'))
        pickler.dump(self)
    
    '''
    Return the frequency of the token (id,string) from self.token_counts
    '''
    def get_token_frequency(self, tokenTuple):    
        try:
            frequency = self.token_counts[tokenTuple]
        except KeyError:
            frequency = 0
        return frequency

    @classmethod
    def getCompoundTokenData(cls, list_of_tokendata):
        ''' Receive a list of TokenData objects, then combine them all and return new object. '''
        T_new = TokenData()
        
        # combine token_to_index
        T_new.no_of_tokens = 0
        for T in list_of_tokendata:
            for token in T.token_2_index:
                
                # If the token already exists in T_new's token_to_index, do nothing. Otherwise, add it and increment no_of_tokens
                try:
                    T_new.token_2_index[token] += 0
                except KeyError:
                    T_new.token_2_index[token] = T_new.no_of_tokens
                    T_new.no_of_tokens += 1
        
        
        # generate index_to_token
        for token, index in T_new.token_2_index.iteritems():
            T_new.index_2_token[index] = token
                     
        
        # compute token_counts
        for token in T_new.token_2_index:
            count = 0
            for T in list_of_tokendata:
                try:
                    count += T.token_counts[token]
                except KeyError:
                    pass
                T_new.token_counts[token] = count
                
        # compute normalized_token_counts
        for T in list_of_tokendata:
            for normalized_token, count in T.normalized_token_counts.iteritems():
                try:
                    # if token already in dict, increment
                    T_new.normalized_token_counts[normalized_token] += count
                except KeyError:
                    # else, initialize with first count
                    T_new.normalized_token_counts[normalized_token] = count
                    
                    
        return T_new            
            
            
        
    
    
        
        
        
        
        
        
        
        
   
