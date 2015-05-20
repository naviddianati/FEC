'''
This module defines a number of classes used to parse and tokenize the data
within records. Most importantly, the L{Tokenizer} class and its subclasses
define methods for "normalizing" various record fields such as first name or
last name. The normalized fields are added to the record as additional dict
key-values. 

After normalizing the fields, Tokenizer also "tokenizes" the recods.
For each field, the value is processed into tokens. Tokenizer defines tokens
as full "words" separated by whitespace while TokenizerNgram defines it as
character n-grams. Based on these tokens, for each record, a "feature vector"
is computed that shows which of the tokens it possesses. These feature vectors
will later allow us to compute LHS hashes for the records which are used to
find similar records efficiently.

The other class in this module is the L{TokenData} class which is instantiated
by the Tokenizer class and contains the global data on the tokens discovered
by Tokenizer such as their frequency, and an index of all tokens.
'''



import cPickle
import json
import os
import pickle
import pprint
import re
from disambiguation.nameparser import HumanName
from address import AddressParser
from nltk.util import ngrams
from .. import config
import disambiguation.data 
from multiprocessing import Manager
import utils



class Tokenizer():
    ''' 
    This class receives a list of records (perhaps retrieved from a MySQL query). 
    The fields are divided between "identifier" fieldsz and "auxiliary" fields.
    The class tokenizes all the identifier fields of all records, and for each 
    record computes a vector that encapsulates this information. The set of these
    vectors is then sent to a Disambiguator instance which computes a similarity 
    graph between the nodes(records) based on their vectors.

    QUESTION: How do we use the auxiliary fields as well?
    '''
   
        
        
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
        
        self.normalized_attrs = ['N_address',
                                 'N_first_name',
                                 'N_last_name',
                                 'N_middle_name',
                                 'N_zipcode',
                                 'N_employer',
                                 'N_occupation']
       
#         # Used when NAME is retrieved from MySQL query, not FIRST_NAME and LAST_NAME
#         self.token_identifiers = {'NAME':[1, 2, 3],
#                                 'CONTRIBUTOR_ZIP':[4],
#                                 'CONTRIBUTOR_STREET_1':[5]}

        self.token_identifiers = self.tokens.token_identifiers
                            
        self.ap = AddressParser()
      
        
        # List of records associated with this Tokenizer.
        self.list_of_records
        
        # Project instance containing data
        self.project = None
        
        # Dictionary {r_id: r's vector}
        self.dict_vectors = {}
        
        
        
        

    
    def _normalize_NAME(self, record):
        ''' 
        this normalizer is applied to the whole name, that is, when
        first/middle/last name and titles are all mixed into one string.
        Uses nameparser.
        '''
        # remove all numerals
        s = record['NAME']

        s1 = re.sub(r'\.|[0-9]+', '', s)
        s1 = re.sub(r'(\bESQ\b)|(\bENG\b)|(\bINC\b)|(\bLLC\b)|(\bLLP\b)|(\bMRS\b)|(\bPHD\b)|(\bSEN\b)', '', s1)
        s1 = re.sub(r'(\bDR\b)|(\bII\b)|(\bIII\b)|(\bIV\b)|(\bJR\b)|(\bMD\b)|(\bMR\b)|(\bMS\b)|(\bMISS\b)|(\bSR\b)|(\bSGT\b)|(\bDC\b)|(\bREV\b)|(\bFR\b)', '', s1)
        s1 = re.sub(r'\.', '', s1)

        name = HumanName(s1)


        record['N_last_name'] = name.last.upper()        
        record['N_first_name'] = name.first.upper()

        # The middle name is sometimes very difficult to pinpoint. If there are multiple pieces, look for a single-letter piece and pick that one
        mn = name.middle.upper()
        if (len(mn) == 1) and (not re.match(r'[A-Z]', mn)):
            record['N_middle_name'] = '' 
            return
        # If there are multiple "chunks", see if there is a single-letter one. Else, pick the first one.
        chunks = re.findall(r'\S+', mn)
        if len(chunks) > 1:
            list_lengths = [len(x) for x in chunks]
            try:
                # Index of the single letter chunk
                index = list_lengths.index(1)
            except ValueError:
                # No single-letter chunk found. Pick the first element
                index = 0 
            record['N_middle_name'] = chunks[index]
        else:
            record['N_middle_name'] = mn
            
        
        
        
        
             
        

   
    def _normalize_NAME_old(self, record):
        ''' 
        This normalizer is applied to the whole name, that is, when
        first/middle/last name and titles are all mixed into one string.
        Void. updates the record. This one doesn't use nameparser. My own version
        '''
        # remove all numerals
        s = record['NAME']
        
        s1 = re.sub(r'\.|[0-9]+', '', s)

        # Suffixes        
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


        
        for s in last_name_list:
            record['N_last_name'].append(s)
            
        for s in first_name_list:
            record['N_first_name'].append(s)
            
        # middle_name is set up to be a string at this point
        if len(middle_name) > 0:
            record['N_middle_name'].append(middle_name[0])
        
     
    def _normalize_LAST_NAME(self, record):
        ''' 
        Not implemented since we mostly use the
        NAME fields of the FEC database instead.
        '''
        pass
    
    def _normalize_FIRST_NAME(self, record):
        ''' 
        Not implemented since we mostly use the
        NAME fields of the FEC database instead.
         '''
        pass
    
    def _normalize_ZIP(self, record):
        # Void. updates the record
        s = record['ZIP_CODE']
        if not s:
            record['N_zipcode'].append(None)                
            return
        record['ZIP_CODE'] = s if len(s) < 5 else s[0:5] 
        record['N_zipcode'].append(record['ZIP_CODE']) 
    
    def _normalize_STREET(self, record):
        # Void. updates the record
        s = record['CONTRIBUTOR_STREET_1']
        try:
            address = self.ap.parse_address(s)
        
            tmp = [address.house_number, address.street_prefix, address.street, address.street_suffix]
            tmp = [x  for x in tmp if x is not None]

            address_new = ' '.join(tmp)
            record["N_address"] = address_new.upper()
        except:
            record["N_address"] = s

   
   
   
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
    
       
   
   
   
   
   
        
    def _get_tokens(self, record, list_fields):
        ''' 
        This function tokenizes a list of selected
        fields for the given record.
        '''        
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
    


    def _get_tokens_FIRST_NAME(self, record):
        name = record['N_first_name']
        # Some first names are contaminated with middle names. clean them up.
        identifier = self.token_identifiers['FIRST_NAME'][0]
        
        
        return [(identifier, s) for s in name]
    

    
    def _get_tokens_LAST_NAME(self, record):
        lastname = record['N_last_name']
        identifier = self.token_identifiers['LAST_NAME'][0]
        return [(identifier, s) for s in lastname]        
    

    
    def _get_tokens_ZIP(self, record):
        zipcode = record['N_zipcode']
        identifier = self.token_identifiers['CONTRIBUTOR_ZIP'][0]
        return [(identifier, s) for s in zipcode]
    

    def _get_tokens_STREET(self, record):
        address = record['N_address']
        if not address:
            return None
        identifier = self.token_identifiers['CONTRIBUTOR_STREET_1'][0]
        return [(identifier, s) for s in address.split()]

        

    
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
     
    
                   
                


     
    def update_normalized_token_counts(self, record):
        '''
        Update self.tokens.normalized_token_counts by tracking/updating
        the frequency of the normalized tokens belonging to the given record.
        '''
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
            
    
    def load_from_file(self):
        '''
        Load the TokenData object and the normalized
        record attributes from file.
        '''
        tokendata_file = config.tokendata_file_template % (self.project['state'], self.__class__.__name__)
        normalized_attributes_file = config.normalized_attributes_file_template % (self.project['state'])
        

        # Attempt to load tokendata from file
        f = open(tokendata_file)
        tokendata = cPickle.load(f)
        self.tokens = tokendata
        f.close()
        
        f = open(normalized_attributes_file)
        dict_normalized_attributes = cPickle.load(f)
        f.close()
        
        counter_error = 0
        for record in self.list_of_records:
            record.tokendata = tokendata
            
            try:
                normalized_attributes = dict_normalized_attributes[record.id]
                # attach the normalized attributes to each record
                for attr in self.normalized_attrs:
                    record[attr] = normalized_attributes[attr]
            except Exception as e:
                print  "blah blah ---- ", e
                counter_error += 1  
        print "Total number of errors: ", counter_error
                    
                
        self.all_token_sorted = sorted(self.tokens.token_counts, key=self.tokens.token_counts.get, reverse=0)

    
            

        
    def tokenize(self, num_procs=1, export_vectors=True, export_normalized_attributes=True, export_tokendata=True):
        '''
        Tokenize the records using either one process or multiple processes.
        @param num_procs: number of processes to use.
        @param export_vectors: whether to export the feature vectors to file.
        '''
        if num_procs == 1:
            self.__tokenize_single_proc(export_vectors, export_normalized_attributes, export_tokendata)
        elif num_procs > 1:
            self.__tokenize_multi_proc(num_procs, export_vectors, export_normalized_attributes, export_tokendata) 
        
    
    
    def __tokenize_multi_proc(self, num_procs, export_vectors, export_normalized_attributes, export_tokendata):
        '''
        Divide the list_of_records into equal chunks. Send the chunks to a
        standalone method. In that method, multiple tokenizer instances will
        be created, and each data chunk assigned to one of them. Each tokenizer
        will then tokenize in a separate process and the results are comgined.
        This includes updating the record vectors held by each tokenizer. The
        combined results will then be returned to this method for saving and
        post processing.
        '''
        from utils import  chunks_replace
        print "splitting list of records..."
        list_of_list_records = chunks_replace(self.list_of_records, num_procs)
        print "done splitting list of records..."

        print "calling tokenizer_multiple_lists_of_records..."        
        self.dict_vectors, self.list_of_records, self.tokens = tokenize_multiple_lists_of_records(list_of_list_records, self.__class__ , self.project)
        
        
        
        if export_tokendata:
            # Export tokendata to file.
            tokendata_file = config.tokendata_file_template % (self.project['state'], self.__class__.__name__)
            self.tokens.save_to_file(tokendata_file)
        
        if export_vectors:
            # Export computed vectors to file
            vectors_file = config.vectors_file_template % (self.project['state'], self.__class__.__name__)
            try:
                f = open(vectors_file, 'w')
                cPickle.dump(self.dict_vectors, f)
                
                f.close()
            except:
                os.remove(vectors_file)
                raise
            self.dict_vectors.clear()
            
        if export_normalized_attributes:
            # Export the normalized attributes to file
            normalized_attributes_file = config.normalized_attributes_file_template % (self.project['state'])
            try:
                f = open(normalized_attributes_file, 'w')
                dict_normalized_attributes = {}
                for r in self.list_of_records:
                    dict_normalized_attributes[r.id] = {}
                    for attr in self.normalized_attrs:
                        try:
                            dict_normalized_attributes[r.id][attr] = r[attr]
                        except KeyError:
                            dict_normalized_attributes[r.id][attr] = None


                cPickle.dump(dict_normalized_attributes, f)
                f.close()
            except:
                os.remove(normalized_attributes_file)
                raise
            dict_normalized_attributes = None
            
                
        self.all_token_sorted = sorted(self.tokens.token_counts, key=self.tokens.token_counts.get, reverse=0)
        print "Total number of tokens identified: ", len(self.tokens.token_counts)

        # set "self.toknes" as the TokenData object associated with each record
        for record in self.list_of_records:
            record.tokendata = self.tokens
        
            
        
        
        
    def __tokenize_single_proc(self, export_vectors, export_normalized_attributes, export_tokendata):
        if not self.list_tokenized_fields:
            raise Exception("ERROR: Specify the fields to be tokenized.")
        
        # The dict of record vectors. It will be computed here and 
        # exported to a file. When needed later, it should be loaded
        # from that file.
        self.dict_vectors = {}
        
        for record in self.list_of_records:
            
            # s_plit is a list of tuples: [(1,'sdfsadf'),(2,'ewre'),...]     
            s_split = self._get_tokens(record, self.list_tokenized_fields)
            
            # A dictionary {token_index: (0 or 1)} showing which tokens exist in the given record
            vector = {}
            
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
                vector[self.tokens.token_2_index[token]] = 1
            self.dict_vectors[record.id] = vector
     
        if export_tokendata:
            # Export tokendata to file.
            tokendata_file = config.tokendata_file_template % (self.project['state'], self.__class__.__name__)
            self.tokens.save_to_file(tokendata_file)
        
        if export_vectors:
            # Export computed vectors to file
            vectors_file = config.vectors_file_template % (self.project['state'], self.__class__.__name__)
            try:
                f = open(vectors_file, 'w')
                cPickle.dump(self.dict_vectors, f)
                
                f.close()
            except:
                os.remove(vectors_file)
                raise
            self.dict_vectors.clear()
            
        
        if export_normalized_attributes:
            # Export the normalized attributes to file
            normalized_attributes_file = config.normalized_attributes_file_template % (self.project['state'])
            try:
                f = open(normalized_attributes_file, 'w')
                dict_normalized_attributes = {r.id:
                                                {attr:r[attr] for attr in self.normalized_attrs} 
                                                for r in self.list_of_records}
                cPickle.dump(dict_normalized_attributes, f)
                f.close()
            except:
                os.remove(normalized_attributes_file)
                raise
            dict_normalized_attributes = None
            
                
        self.all_token_sorted = sorted(self.tokens.token_counts, key=self.tokens.token_counts.get, reverse=0)
        print "Total number of tokens identified: ", len(self.tokens.token_counts)

        # set "self.toknes" as the TokenData object associated with each record
        for record in self.list_of_records:
            record.tokendata = self.tokens


    
  
    def setTokenizedFields(self, list_tokenized_fields):
        '''
        List of MySQL table "identifier" fields retrieved
        using the original MySQL query
        '''
        self.list_tokenized_fields = list_tokenized_fields
    
  


          
    def setRecords(self, list_of_records):
            self.list_of_records = list_of_records
        
    def getRecords(self):
        return self.list_of_records   

#===========================================================================
#     Class containing all token data
#===========================================================================



class TokenizerNgram(Tokenizer):
    '''
    Subclass of Tokenizer that implements different 
    tokenizer functions for FIRST_NAME,LAST_NAME,etc.
    '''
    
    def __init__(self):
        Tokenizer.__init__(self)
        
        # Features are bigrams
        self.ngram_n = 2
       
       
    def ngrams(self, sentence, n):
        '''
        split string into tokens and then extract
        all ngrams in all tokens.
         '''
        list_ngrams = []
        tokens = sentence.split()
        for token in tokens:
            myngrams = ngrams(token, n)
            for grams in myngrams:
                list_ngrams.append("".join(grams)) 
        return list_ngrams
    
    def _get_tokens_NAME(self, record):
        '''
        Override: here, tokenz are ngrams.
        NOTE: with the new name normalizer, each of the
        following are a single string: N_last_name, N_first_name,N_middle_name.
        '''
        
        
        lastname = record['N_last_name']
        firstname = record['N_first_name']
        middlename = record['N_middle_name']
        
        # Get the "word tokens" and then add ngrams below
        tokens = Tokenizer._get_tokens_NAME(self, record)
        
        # last name
        identifier = self.token_identifiers['NAME'][0]
        
        tokens += [(identifier, s) for  s in self.ngrams(lastname, self.ngram_n) ]

        
        # first name
        identifier = self.token_identifiers['NAME'][1]
        tokens += [(identifier, s) for  s in self.ngrams(firstname, self.ngram_n) ]
        
        
        # No need anymore. The superclass method takes care of the middle name        
        # middle name
        # identifier = self.token_identifiers['NAME'][2]
        # if len(middlename) > 0:
        #    tokens += [(identifier, middlename[0])]
    
        return tokens    
        
                
    

    def _normalize_STREET(self, record):
        # Override.
        s = record['CONTRIBUTOR_STREET_1']
        if not s: 
            record["N_address"] = s
            return

        # Treat special cases
        # Leave P.O. Boxes alone
        if re.match(r'\bP\.?O\.?\b|\bbox\b|p\.?o\.?box', s, re.IGNORECASE):
            record["N_address"] = s
            return           
        
        try:
            address = self.ap.parse_address(s)
        
            tmp = [address.house_number, address.street_prefix, address.street, address.street_suffix]
            tmp = [x  for x in tmp if x is not None]

            address_new = ' '.join(tmp)
            record["N_address"] = address_new.upper()
        except:
            record["N_address"] = s

        # Check if normalizer screwed up too badly
        # If final string length is less than 5, then use the raw string
        if len(record["N_address"]) < 5:
            record["N_address"] = s
        
        

   



    def _get_tokens_FIRST_NAME(self, record):
        # Override: here, tokens are ngrams
        firstname = record['N_first_name']
        identifier = self.token_identifiers['NAME'][1]
        tokens = [(identifier, s) for word in firstname for myngram in self.ngrams(word, self.ngram_n) for s in myngram]
        return  tokens
    
    
    def _get_tokens_LAST_NAME(self, record):
        # Override: here, tokens are ngrams
        lastname = record['N_last_name']
        identifier = self.token_identifiers['NAME'][0]
        tokens = [(identifier, s) for word in lastname for myngram in self.ngrams(word, self.ngram_n) for s in myngram]
        return  tokens
    
   
    def _get_tokens_ZIP(self, record):
        return Tokenizer._get_tokens_ZIP(self, record)
    
   
    # override
    def _get_tokens_STREET(self, record):
        return Tokenizer._get_tokens_STREET(self, record)

        


class TokenData():
    '''
    This class encapsulates the global token data derived
    from all records processed by a Tokenizer instance.
    '''

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
        with open(disambiguation.data.DICT_PATH_DATAFILES['name-variants.json']) as f:
            self.dict_name_variants = json.load(f)
        
        with open(disambiguation.data.DICT_PATH_DATAFILES['all-names.json']) as f:
            self.set_all_names = set(json.load(f))
        
        self.token_identifiers = {'NAME':[1, 2, 3],
                                   'N_full_name':[123],
                                   'N_last_name': [1],
                                   'LAST_NAME':[1],
                                   'N_first_name':[2],
                                   'FIRST_NAME':[2],
                                   'N_middle_name':[3],
                                   'MIDDLE_NAME':[3],
                                   'CONTRIBUTOR_ZIP':[4],
                                   'ZIP_CODE':[4],
                                   'CONTRIBUTOR_STREET_1':[5],
                                   'N_occupation':[6],
                                   'OCCUPATION':[6],
                                   'N_employer':[7],
                                   'EMPLOYER': [7]}
    
    def save_to_file(self, filename):
        f = open(filename, 'w')
        pickler = pickle.Pickler(f)
        pickler.dump(self)
        f.close()
    
    def get_token_frequency(self, tokenTuple):    
        '''
        Return the frequency of the token (id,string)
        from self.token_counts.
        '''
        try:
            frequency = self.token_counts[tokenTuple]
        except KeyError:
            frequency = 0
        return frequency
    

    @classmethod
    def getCompoundTokenData(cls, list_of_tokendata):
        ''' 
        Receive a list of TokenData objects, then combine 
        them all and return new object. 
        '''
        
        T_new = TokenData()
        
        # combine token_to_index
        T_new.no_of_tokens = 0
        for T in list_of_tokendata:
            for token in T.token_2_index:
                
                # If the token already exists in T_new's token_to_index,
                # do nothing. Otherwise, add it and increment no_of_tokens
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
            
            
        
    


        
        
        
   












import Project


def worker_tokenizer_list_of_records(data):
    '''
    Worker function that receives a dict of records, creates a new TokenizerClass
    instance and tokenizes the records. Then it returns the dict_of_vectors to
    be combined with other similar objects from other workers.
    '''
    
    utils.time.sleep(20)
    return
    list_of_records, fields, TokenizerClass = data
    print "--------", TokenizerClass
    
    tokenizer = TokenizerClass()
    
    print "tokenizer initialized"
    project = Project.Project(1)
    print "project initialized"
    project.tokenizer = tokenizer
    tokenizer.project = project
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(fields)
    
    
    print "Tokenizing records..."
    tokenizer.tokenize(num_procs=1, export_vectors=False, export_normalized_attributes=False, export_tokendata=False)
    
    print "Saving token data to file..."
#     tokenizer.tokens.save_to_file(tokendata_file)
#     list_of_records = tokenizer.getRecords()
    return tokenizer.getRecords(), tokenizer.dict_vectors, tokenizer.tokens

    

def tokenize_multiple_lists_of_records(list_of_list_records, TokenizerClass, project):
    '''
    Receive multiple chunks of the tokenizer's list_of_records, instantiate a new
    TokenizerClass for each and tokenize the chunks in separate processes, then combine
    and update their record vectors and other attributes.
    @param list_of_list_records: a list where each element is a contiguous chunk of
        list_of_records where list_of_records is that of the original tokenizer that
        calls this method in order to tokenize with multiple processes.
    '''
    import gc
    num_procs = len(list_of_list_records)
    list_data = []
    manager = utils.multiprocessing.Manager() 

    myrecord = list_of_list_records[0][0]
    print "preparing data packages for child processes..."
    for i in range(len(list_of_list_records)):
        print "---- data for process ", i
        # list_data.append((manager.list(list_of_list_records[0]), project['list_tokenized_fields'] ,TokenizerClass))
        list_data.append((list_of_list_records[0], project['list_tokenized_fields'] , TokenizerClass))
        # del list_of_list_records[0][:]
        del list_of_list_records[0]

    
    
    print "starting the pool..."
    pool = utils.multiprocessing.Pool(processes=num_procs)
    result = pool.map(worker_tokenizer_list_of_records, list_data)
    
    list_of_list_records = [item[0] for item in result]
    list_dict_vectors = [item[1] for item in result]
    list_tokendata = [item[2] for item in result]

    # combine results
    compound_tokendata = TokenData.getCompoundTokenData(list_tokendata)
    compound_dict_vectors = {}
    compound_list_of_records = []
    
    # concatenate the new list_of_records objects, and delete each
    # list_of_record from list_of_list_records.
    for i in range(len(list_of_list_records)):       
        compound_list_of_records += list_of_list_records[0]
        del list_of_list_records[0][:]
        del list_of_list_records[0]
        
    
    # update vectors
    for chunk_id, dict_vectors in enumerate(list_dict_vectors):
        old_tokendata = list_tokendata[chunk_id]
        for r_id, old_vector in dict_vectors.iteritems():
            vector = {}
            # translate self.vector
            for index_old in old_vector:
                index_new = compound_tokendata.token_2_index[old_tokendata.index_2_token[index_old]]
                vector[index_new] = 1
             
            compound_dict_vectors[r_id] = vector

    
    print "total number of records:", len(compound_list_of_records)
    return compound_dict_vectors, compound_list_of_records, compound_tokendata













if __name__ == "__main__":
    pass













