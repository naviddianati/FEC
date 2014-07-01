'''
Created on Jun 25, 2014

@author: navid
'''
import json

import editdist


class Record(dict):
    def __init__(self):
        # vector is a a sparse vector, namely a dictionary.
        self.vector = {}
        
        # The TokenData object containing the token_2_index and index_2_token and other tokenization data relevant to this record.
        # Should be set somewhere before the record is compared.
        # Here, it's set in Tonekizer's getRecords() method which returns the list of records after attaching
        # the TokenData object to each one of them.
        self.tokendata = None
        # Therefore, its dimension should be stored separately.
        self.dim = 0        

    
#     @staticmethod
#     def comparison(record1, record2):
#         return Record._is_close_enough(record1.vector, record2.vector) 
#     
    def compare(self, otherRecord):
        return self._is_close_enough(self.vector, otherRecord.vector)
    
    def __getitem__(self, key):
        val = dict.__getitem__(self, key)
        return val

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)
        
   

  
    def _is_close_enough(self, v1, v2):
        ''' This function returns True or False indicating whether two name vectors are close enough to be considered identical or not '''
        ''' If one has no address, then comparison should be much more strict.
            If both have addresses, then addresses should be idenetical, and other fields must be close enough.'''
        identical = True
#         return identical
        # Generate dictionary of lastname, firstname, middlename and suffix tokens for both vectors
        dict1 = {}
        dict1[1] = []  # Last name
        dict1[2] = []  # First nameb
        dict1[3] = []  # Middle name
        dict1[4] = []  # suffix   
        dict1[5] = []  # address      
     
        for index in v1:
            token = self.tokendata.index_2_token[index]
            if token[0] not in dict1: dict1[token[0]] = []
            dict1[token[0]].append(token[1])
#         print json.dumps(dict1)
        
        dict2 = {}
        dict2[1] = []  # Last name
        dict2[2] = []  # First name
        dict2[3] = []  # Middle name
        dict2[4] = []  # suffix  
        dict2[5] = []  # address      
        for index in v2:
            token = self.tokendata.index_2_token[index]
            if token[0] not in dict2: dict2[token[0]] = []
            dict2[token[0]].append(token[1])
        
        # If either has no address, require firstname and last name to be identical and also middle name if both have it.
        if not dict1[5] or not  dict2[5]:
            identical = (dict2[1] == dict1[1]) and (dict2[2] == dict1[2]) and ((dict2[3] == dict1[3]) if (dict2[3] and dict1[3]) else True)
            return identical
        
        # IF BOTH HAVE ADDRESSES:
                       
        # If street addresses aren't identical, then fail
        if dict1[5] != dict2[5]: 
            identical = False;
            return identical
              
        
        # if both have middlenames, they should be the same
        if dict1[3] and dict2[3]:
            if dict1[3] != dict2[3]: identical = False
        
        # if 1 doesn't have a middle name but 2 does, then 2 is not the "parent" of 1
        if not dict1[3] and dict2[3]: identical = False
          
        # if last names arden't close enough, fail.
#         if dict1[1] != dict2[1]: identical = False;
        if not dict2[1] or not dict1[1]: 
            identical = False       
        elif editdist.distance(dict1[1][0], dict2[1][0]) > 2: identical = False

        # if first names don't overlap, then check if they are variants. If not, fail
#         if not any(i in dict1[2] for i in dict2[2]): identical = False
        firstname1 = ' '.join(dict1[2])
        firstname2 = ' '.join(dict2[2])  
        if editdist.distance(firstname1, firstname2) > 1:
            if firstname2 in self.tokendata.dict_name_variants:
                if firstname1 not in self.tokendata.dict_name_variants[firstname2]: 
                    identical = False
            else:
                identical = False
        
        return identical
                   
        
        
    



         

