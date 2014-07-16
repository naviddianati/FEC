'''
Created on Jun 25, 2014

@author: navid
'''
'''
Hashable class.
'''

import json

from Affiliations import bad_identifier
from Person import Person
import editdist


class Record(dict):
    def __init__(self):
        # a dictionary {token_index: (0 or 1)} showing which tokens exist in the given record.
        # A token index is an integer assigned to each unique tuple (token_identifier, string) where a token identifier is an integer
        # indicating the type of the token (Last_NAME? FIRST_NAME? CONTRIBUTOR_ZIP? etc?)
        # vector is a a sparse vector, namely a dictionary.
        self.vector = {}
        
        # Therefore, its dimension should be stored separately.
        self.dim = 0
        
        # unique identifier of the record. Will be set at the time of retrieval from database
        self.index = None
        
        
        # The Person object associated with the record. This attribute will be assigned during the process of disambiguation.
        # TODO: how exactly will this be decided? Idea: Give each record an identity, then merge the identities. As a record's identity
        #     references a different Person object, its old Person object will be scheduled for garbage collection.
        self.identity = None   
        
        # The TokenData object containing the token_2_index and index_2_token and other tokenization data relevant to this record.
        # Should be set somewhere before the record is compared.
        # Here, it's set in Tonekizer's getRecords() method which returns the list of records after attaching
        # the TokenData object to each one of them.
        self.tokendata = None

        # Graph objects containing the global affiliation network data. They can be used 
        # to perform comparison between records.
        # Each graph object G contains a dictionary called dict_string_2_ind which maps
        # the affiliation strings to the index of the corresponding vertex in the affiliation
        # graph to be used for fast access to the vertex.
        self.G_employer = None
        self.G_occupation = None
        
        # Normalized attributes. Used for detailed pairwise comparison.
        # The Tokenizer can compute and load these attributes
        self['N_first_name'] = []
        self['N_last_name'] = []
        self['N_middle_name'] = []
        self['N_address'] = []
        self['N_zipcode'] = []
        
        # Print debug info or not.
        self.debug = False

    
    ''' The follosing two methods need to be defined in order to make the object hashable.
    Note that my definition of __eq__ works fine as long as I don't ever need to compare
    equality between two objects based on their "content".
    '''
    
    # Override
    def __hash_(self):
        return id(self)
    
    # Override
    def __eq__(self, other):
        return  (id(self) == id(other)) 
    
    
    
    
#     @staticmethod
#     def comparison(record1, record2):
#         return Record._is_close_enough(record1.vector, record2.vector) 
#     
    def compare(self, otherRecord, mode="strict"):
        if mode == "strict":
            return self._is_close_enough_STRICT(self, otherRecord)
        
        if mode == "thorough":
            decision = self._is_close_enough_THOROUGH(self, otherRecord)
            if self.debug: 
                print decision
            return decision
        
        raise ValueError("Specify comparison mode") 
    
    def __getitem__(self, key):
        val = dict.__getitem__(self, key)
        return val

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)
        
   
        
        
    
    def _is_close_enough_STRICT(self, r1, r2):
        ''' This function returns True or False indicating whether two name vectors are close enough to be considered identical or not '''
        ''' If one has no address, then comparison should be much more strict.
            If both have addresses, then addresses should be idenetical, and other fields must be close enough.'''
        
        if r1.G_employer and r2.G_employer:
            # TODO: use employer field in comparison
            pass
        
        if r1.G_occupation and r2.G_occupation:
            # TODO: use occupation field in comparison
            pass
        
        
        identical = True
     
        
        # If either has no address, require firstname and last name to be identical and also middle name if both have it.
        if not r1['N_address'] or not  r2['N_address']:
            identical = (r1['N_last_name'] == r2['N_last_name']) \
                        and (r1['N_first_name'] == r2['N_first_name']) \
                        and ((r1['N_middle_name'] == r2['N_middle_name']) \
                            if (r1['N_middle_name'] and r2['N_middle_name']) else True)
            return identical
        
        # IF BOTH HAVE ADDRESSES:
                       
        # If street addresses aren't identical, then fail
        if r1['N_address'] != r2['N_address'] : 
            identical = False;
            return identical
              
        
        # if both have middlenames, they should be the same
        if r1['N_middle_name'] and r2['N_middle_name']:
            if r1['N_middle_name'] != r2['N_middle_name']: identical = False
        
        # if 1 doesn't have a middle name but 2 does, then 2 is not the "parent" of 1
        if not r1['N_middle_name'] and r2['N_middle_name']: identical = False
          
        # if last names arden't close enough, fail.
#         if dict1[1] != dict2[1]: identical = False;
        if not r1['N_last_name'] or not r2['N_last_name']: 
            identical = False       
        elif editdist.distance(''.join(sorted(r1['N_last_name'])), ''.join(sorted(r2['N_last_name']))) > 2: identical = False

        # if first names don't overlap, then check if they are variants. If not, fail
#         if not any(i in dict1[2] for i in dict2[2]): identical = False
        firstname1 = ' '.join(r1['N_first_name'])
        firstname2 = ' '.join(r2['N_first_name'])  
        if editdist.distance(firstname1, firstname2) > 1:
            if firstname2 in self.tokendata.dict_name_variants:
                if firstname1 not in self.tokendata.dict_name_variants[firstname2]: 
                    identical = False
            else:
                identical = False
        
        return identical
                   
                   
                   
    # TODO: implement the new comparison function
    def _is_close_enough_THOROUGH(self, r1, r2):
#         self.debug = True if   r1['N_first_name'] == r2['N_first_name'] == "MARKUS" and r1['N_last_name'] == r2['N_last_name'] == "AAKKO" else False
#         self.debug = True  if r1['N_first_name'] != r2['N_first_name']  else False;  
        if self.debug:
            print "_______________________________________________________________________________________________________________"
            print r1
            print r2
            # If states are the same,
        
        
        if r1['STATE'] == r2['STATE']:
            
            # If cities are the same
            if r1['CITY'] == r2['CITY']:
            
                # If both have addresses
                if r1['N_address'] and r2['N_address']:
                    
                    # If addresses are the same
                    if r1['N_address'] == r2['N_address']:
                        # TODO: if states are the same and addresses are the same
                        return self.compare_names(r1, r2)
                    
                    # If addresses aren't the same
                    else:
                        # TODO: if states and cities are the same but addresses are different
                        # Accept if zipcodes are the same and at least one of the affiliations is clearly related and exact same name
                        return (self.compare_employers(r1, r2) >= 2 or  self.compare_occupations(r1, r2) >= 2)\
                            and self.compare_names(r1, r2) and self.compare_zipcodes(r1, r2)                
                
                
                # if at least one doesn't have an address
                # TODO: if states and cities are the same, but at least one doesn't have an address
                else:
                    # if zip codes are the same, then relax the affiliation condition a bit
                    if self.compare_zipcodes(r1, r2) == 2:
                        return (self.compare_employers(r1, r2) >= 1 or  self.compare_occupations(r1, r2) >= 1)\
                            and self.compare_names(r1, r2)
                    else:
                        # If at least one doesn't have an address and zipcodes are different
                        # Accept if zipcodes are the same and at least one of the affiliations is clearly related and exact same name
                        return (self.compare_employers(r1, r2) >= 2 and  self.compare_occupations(r1, r2) >= 2)\
                                and self.compare_names(r1, r2) 

            # If states are the same but cities are different
            else:
                # TODO: if cities are different
                # Accept if affiliations are clearly connected and names are exactly the same
                # TODO: check for timeline consistency
                return ((self.compare_employers(r1, r2) >= 2 and  self.compare_occupations(r1, r2) >= 2)\
                          and self.compare_names(r1, r2))
                

            
        # If states are DIFFERENT, 
        else:
            # TODO: If states are DIFFERENT, 
            # 1- Names should be very close.
            # 2- Occupations should be close
            # 3- Employers should be close
            # 4- Name token frequency should be taken into account. 
            # 5- Check for timeline consistency: Requires the Person objects
            pass
        return False
    
    
    
    '''Returns a number:
        0: zipcodes are different
        1: at least one doesn't have a zipcode
        2: zip codes are the same.'''
    def compare_zipcodes(self, r1, r2):
        if not r1['ZIP_CODE'] or not r2["ZIP_CODE"]:
            if self.debug: print "One ZIP_CODE doesn't exist"
            return 1
        if r1['ZIP_CODE'][:5] == r2["ZIP_CODE"][:5]:
            return 2
        else:
            return 0
             
    
    '''
    Returns a number:
        0: they both exist but are unrelated
        1: at least one doesn't have the field
        2: connected in the affiliations network
        3: exactly the same'''
    
    def compare_occupations(self, r1, r2):
        try:
            occupation1 = r1['OCCUPATION']
        except KeyError:
            if self.debug: print "no occupation field found"
            return 1
        
        try:
            occupation2 = r2['OCCUPATION']
        except KeyError:
            if self.debug: print "no occupation field found"
            return 1
        
#         print occupation1, occupation2
        
#         if bad_identifier(occupation1, type="occupation") or bad_identifier(occupation2, type="occupation"):
#             pass
#             return False
        
        if occupation1 == occupation2:
            if self.debug: print "occupations are the same" 
            return 3
        else:
            try:
                ind1 = self.G_occupation.dict_string_2_ind[occupation1]
                ind2 = self.G_occupation.dict_string_2_ind[occupation2]
            except KeyError:
                if self.debug: print "one of the occupations not found"
                return 1
            
            
            # Check if the occupation identifiers are adjacent in the affiliations graph
            if self.G_occupation.get_eid(ind1, ind2, directed=False, error=False) == -1:
                if self.debug: print "occupations are different and not adjacent"
                return 0 
            else:
                if self.debug: print "-------------", occupation1, occupation2
                return 2
        
    
    
    ''' Right now, only return True if occupations are exactly the same
    Returns a number:
        0: they both exist but are unrelated
        1: at least one doesn't have the field
        2: connected in the affiliations network
        3: exactly the same'''
    def compare_employers(self, r1, r2):
        try:
            employer1 = r1['EMPLOYER']
        except KeyError:
            if self.debug: print "no employer field found"
            return 1
        
        try:
            employer2 = r2['EMPLOYER']
        except KeyError:
            if self.debug: print "no employer field found"
            return 1
        
#         print employer1,employer2
        
#         if bad_identifier(employer1, type="employer") or bad_identifier(employer2, type="employer"):
#             pass
#             return False
        
        if employer1 == employer2:
            if self.debug: print "employers are the same"
            return 3
        else:
            try:
                ind1 = self.G_employer.dict_string_2_ind[employer1]
                ind2 = self.G_employer.dict_string_2_ind[employer2]
            except KeyError:
                if self.debug: print "one of the employers not found"
                return 1
            
            # Check if the employer identifiers are adjacent in the affiliations graph
            if self.G_employer.get_eid(ind1, ind2, directed=False, error=False) == -1:
                if self.debug: print "employers are not adjacent"
                return 0
            else:
                if self.debug: print "-------------", employer1, employer2
                return 2
    
    
    def compare_names(self, r1, r2):
        identical = True
        # if both have middlenames, they should be the same
        if r1['N_middle_name'] and r2['N_middle_name']:
            if r1['N_middle_name'] != r2['N_middle_name']: 
                if self.debug: print "middle names are different"
                identical = False
                return identical
        
#         # if 1 doesn't have a middle name but 2 does, then 2 is not the "parent" of 1
#         if not r1['N_middle_name'] and r2['N_middle_name']: 
#             identical = False
#             return identical
          
        # if last names aren't close enough, fail.
        if not r1['N_last_name'] or not r2['N_last_name']: 
            if self.debug:
                print "one of the last names doesn't exist"
            identical = False 
            return identical      
        # TODO: if both have last names takce into account their frequencies
        elif editdist.distance(''.join(sorted(r1['N_last_name'])), ''.join(sorted(r2['N_last_name']))) > 2: 
            identical = False
            if self.debug:
                print "Distance between last names too far"
            return identical      

        # if first names don't overlap, then check if they are variants. If not, fail
#         if not any(i in dict1[2] for i in dict2[2]): identical = False
        firstname1 = r1['N_first_name']
        firstname2 = r2['N_first_name']
        
        if editdist.distance(firstname1, firstname2) > 1:
            if self.debug: print "First names are different"
            one_var_two = None
            two_var_one = None
#             print firstname1,"-----",firstname2  
            
            # Check if firstname1 is a variant of firstname2
            if firstname2 in self.tokendata.dict_name_variants:
                if firstname1 not in self.tokendata.dict_name_variants[firstname2]: 
                    one_var_two = False
                else:
                    one_var_two = True

            # Check if firstname2 is a variant of firstname1
            if firstname1 in self.tokendata.dict_name_variants:
                if firstname2 not in self.tokendata.dict_name_variants[firstname1]: 
                    two_var_one = False
                else:
                    two_var_one = True
            
            # If at least one of them is registered as the other's variant:
            if one_var_two or two_var_one:
                if self.debug: print "first names ARE variants of each other"
                identical = True
            else:
                if self.debug: print "first names aren't variants of each other"
                identical = False
        
        return identical
        
        
                    
            
                    
        
            

         

