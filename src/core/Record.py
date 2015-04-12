'''
Created on Jun 25, 2014

@author: navid
'''
'''
Hashable class.
'''


'''
When can we use name frequency in record comparison?

1- city and zipcode are the same, some affiliation is missing:
    ________________________________________________________________________________
                        0       1      2     3                      4  5
    0    BEAZLEY, MICHAEL  TOLEDO  43606  SELF             CONSULTANT
    1  BEAZLEY, MICHAEL J  TOLEDO  43606        INFORMATION REQUESTED  J



1- city and zipcode are different, no meaningful affiliation info, bu name VERY rare
    ________________________________________________________________________________
                          0            1      2                    3        4  5
    0    CANCELLIERE, LAURA  BAY VILLAGE  44140              RETIRED  RETIRED
    1  CANCELLIERE, LAURA N     WESTLAKE  44145  COMMUNITY VOLUNTEER           N


1- CITY1 == CITY2:




'''

import json

from common import bad_identifier
from Person import Person
from Tokenizer import TokenData
import editdist


class Record(dict):
    
    # Print debug info or not.
    debug = False 
    
    LARGE_NEGATIVE = -1000
    LARGE_POSITIVE = 1000
    
    
    # strings are considered equivalent if editdist(s1,s2) < self.employer_str_tolerance * max(len(s1),len(s2))
    # tolerance 0.5 is very lax, 0.1 is stringent
    employer_str_tolerance = 0.3
    occupation_str_tolerance = 0.2
    
    def __init__(self):
        # a dictionary {token_index: (0 or 1)} showing which tokens exist in the given record.
        # A token id is an integer assigned to each unique tuple (token_identifier, string) where a token identifier is an integer
        # indicating the type of the token (Last_NAME? FIRST_NAME? CONTRIBUTOR_ZIP? etc?)
        # vector is a a sparse vector, namely a dictionary.
        self.vector = {}
        
        # Therefore, its dimension should be stored separately.
        self.dim = 0
        
        # unique identifier of the record. Will be set at the time of retrieval from database
        self.id = None
        
        
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
        # the affiliation strings to the id of the corresponding vertex in the affiliation
        # graph to be used for fast access to the vertex.
        
        self.list_G_employer = []
        self.list_G_occupation = []
        
        # Normalized attributes. Used for detailed pairwise comparison.
        # The Tokenizer can compute and load these attributes
        self['N_first_name'] = []
        self['N_last_name'] = []
        self['N_middle_name'] = []
        self['N_address'] = []
        self['N_zipcode'] = []
      

    
    ''' The follosing two methods need to be defined in order to make the object hashable.
    Note that my definition of __eq__ works fine as long as I don't ever need to compare
    equality between two objects based on their "content".
    '''
    
    # Override
    def __hash__(self):
        return id(self)
    
    # Override
    def __eq__(self, other):
        return  (id(self) == id(other)) 
    
    
    
    '''
    return a short string summarizing the record
    '''
    def toString(self):
        s = self['NAME']
        return s
    
#     @staticmethod
#     def comparison(record1, record2):
#         return Record._is_close_enough(record1.vector, record2.vector) 
#     

    '''
    Returns an integer. Your code must interpret it by itself!
    Positive values are True while others are False.
    Negative numbers can be used to indicate strict irreconcilability.
    '''
    def compare(self, otherRecord, mode="strict_address"):
        
        
        if mode == "strict_address":
            return self._compare_STRICT_ADDRESS(self, otherRecord) 
        
        elif mode == "strict_affiliation":
            return self._compare_STRICT_AFFILIATION(self, otherRecord) 
        
        elif mode == "thorough":
            decision = (self._compare_THOROUGH(self, otherRecord))
            if Record.debug: 
                print decision
            return decision
        
        
        elif mode == "1":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=1))
            return decision
        elif mode == "2":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=2))
            return decision 
        elif mode == "3":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=3))
            return decision
        elif mode == "4":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=4))
            return decision
        elif mode == "5":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=5))
            return decision
        elif mode == "6":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=6))
            return decision
        elif mode == "7":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=7))
            return decision
        elif mode == "8":
            decision = (self._compare_THOROUGH(self, otherRecord, method_id=8))
            return decision
        
        
        else:
            raise ValueError("Specify comparison mode") 
    
    
    
    def __getitem__(self, key):
        val = dict.__getitem__(self, key)
        return val

    def __setitem__(self, key, val):
        dict.__setitem__(self, key, val)
        
   
        
        
    
    def _compare_STRICT_ADDRESS(self, r1, r2):
        ''' This function returns two values: the verdict, and the detailed result of the  comparisons performed.
        TODO: return the result (details) too. Right now, None is returned for the detailed result.

        This function returns True or False indicating whether two name vectors are close enough to be considered identical or not.
        If one has no address, then comparison should be much more strict.
        If both have addresses, then addresses should be idenetical, and other fields must be close enough.'''
        
        
        identical = True
     
        
        # If either has no address, require firstname and last name to be identical and also middle name if both have it.
        # WHY DID I DO THIS?! MAKES NO SENSE!!!!!
        #if not r1['N_address'] or not  r2['N_address']:
        #    identical = (r1['N_last_name'] == r2['N_last_name']) \
        #                and (r1['N_first_name'] == r2['N_first_name']) \
        #                and ((r1['N_middle_name'] == r2['N_middle_name']) \
        #                    if (r1['N_middle_name'] and r2['N_middle_name']) else True)
        #    return identical, None


        # Both records MUST have addresses
        if not r1['N_address'] or not  r2['N_address']:
            identical = False
            return identical,None
            

        
        # IF BOTH HAVE ADDRESSES:
                       
        # If street addresses aren't identical, then fail
        if r1['N_address'] != r2['N_address'] : 
            identical = False;
            return identical, None
              
        
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
        
        return identical, None
                   



    def _compare_STRICT_AFFILIATION(self, r1, r2):
        ''' This function returns two values: the verdict, and the detailed result of the  comparisons performed.
        TODO: return the result (details) too. Right now, None is returned for the detailed result.

        This function returns True or False indicating whether two name vectors are close enough to be considered identical or not.
       ''' 
        
        
        identical = True
     
        
                       
        # If employers aren't identical, then fail
        if r1['EMPLOYER'] != r2['EMPLOYER'] : 
            identical = False;
            return identical, None
 

        # If occupations aren't identical, then fail
        if r1['OCCUPATION'] != r2['OCCUPATION'] : 
            identical = False;
            return identical, None
              

        # If the affiliations were identical but empty, then fail
        if (not r1['OCCUPATION']) or (not r1['EMPLOYER']):
            identical = False;
            return identical, None
        
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
        
        return identical, None
                   
#     
#     def pvalue(self, r1, r2):
#         
#         f1 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['LAST_NAME'][0], r1['N_last_name']))
#         f2 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['LAST_NAME'][0], r2['N_last_name']))
#         
#         g1 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['FIRST_NAME'][0], r1['N_first_name']))
#         g2 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['FIRST_NAME'][0], r2['N_first_name']))
#         
#         
    
    def get_name_pvalue(self, which='firstname'):
        '''Return the p-value of the firstname-lastname combination given the
        null hypothesis that first names and last names are selected randomly
        in such a way that the token frequencies are what we observe.'''
        record = self
        try:
            if which == 'firstname':
                f2 = record.tokendata.get_token_frequency((2, record['N_first_name']))
                total = record.tokendata.no_of_tokens
                return 1.0 * f2 / total 
            
            if which == 'lastname':
                f1 = record.tokendata.get_token_frequency((1, record['N_last_name']))
                total = record.tokendata.no_of_tokens
                return 1.0 * f1 / total 

        except Exception as e:
            print e
            return None   


                   

    

    def _verdict_addressN_zipY(self, c_n, c_e, c_o, method_id=None):
        ''' If addresses are different and zipcodes are the same'''
        if method_id is None:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        
        # strict
        elif method_id == 1:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 2:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 3:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 4:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        
        
        elif method_id == 5:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        elif method_id == 6:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        elif method_id == 7:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        elif method_id == 8:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
       


    def _verdict_addressN_zipN(self, c_n, c_e, c_o, method_id=None):
        ''' If addresses are different and zipcodes are different'''
        if method_id is None:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)

        elif method_id == 1:
            return (c_e >= 3 and c_o >= 3) and (c_n == 3)
        elif method_id == 2:
            return (c_e >= 3 and c_o >= 3) and (c_n == 3)
        
        elif method_id == 3:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 4:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        
        elif method_id == 5:
            return (c_e >= 3 and c_o >= 3) and (c_n == 3)
        elif method_id == 6:
            return (c_e >= 3 and c_o >= 3) and (c_n == 3)
        
        elif method_id == 7:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 8:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
       
        


    def _verdict_addressNA_zipY(self, c_n, c_e, c_o, method_id=None):        
        ''' If at least one address is None  and zipcodes are the same'''
        if method_id is None:
            return (c_e >= 1 or c_o >= 1) and (c_n == 3)
       
        elif method_id == 1:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        elif method_id == 2:
            return (c_e >= 1 or c_o >= 1) and (c_n == 3)
        
        elif method_id == 3:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        elif method_id == 4:
            return (c_e >= 1 or c_o >= 1) and (c_n == 3)
        
        elif method_id == 5:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        elif method_id == 6:
            return (c_e >= 1 or c_o >= 1) and (c_n == 3)
        
        elif method_id == 7:
            return (c_e >= 2 or c_o >= 2) and (c_n == 3)
        elif method_id == 8:
            return (c_e >= 1 or c_o >= 1) and (c_n == 3)
        


    def _verdict_addressNA_zipN(self, c_n, c_e, c_o, method_id=None):
        ''' If at least one address is None  and zipcodes are different'''
        # All are the same
        if method_id is None:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 1:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 2:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 3:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 4:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 5:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 6:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 7:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 8:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        


    def _verdict_cityN(self, c_n, c_e, c_o, method_id=None):
        ''' If cities are different '''        
        # All are the same
        if method_id is None:
            return ((c_e >= 2 and c_o >= 2) or (c_o == 3) or (c_e == 3)) and (c_n == 3)
        elif method_id == 1:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 2:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 3:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 4:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 5:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 6:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 7:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        elif method_id == 8:
            return (c_e >= 2 and c_o >= 2) and (c_n == 3)
        


    def _verdict_stateN(self, c_n, c_e, c_o, method_id=None):
        ''' If states are different '''
        # All are the same
        if method_id is None:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 1:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 2:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 4:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 3:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 4:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 5:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 6:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 7:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
        elif method_id == 8:
            return (c_e >= 2 or c_o >= 2) and (c_n ==3 )
       


    def _compare_THOROUGH(self, r1, r2, method_id=None):
        '''
        Returns an integer and a dictionary. One can use this function directly to get additional information about
        the nature of the relationship. For instance, this function can return negative numbers to
        indicate irreconcilable difference.

        The "result" dictionary returns the details of the sub-comparisons, namely, the return values of the
        name, occupation and employer comparison functions used to reach the verdict.

        TODO:
        1- (DONE) full middlenames and middlename initials aren't matched correctly
        2- (DONE) CITY misspellings aren't detected. Pay attention to zipcode in this case. Maybe if zipcodes are the same,
            treat as if cities are the same.
        3- When city and zipcode are slightly different but affiliations are the same, should accept.
            When affiliaiton score is 1, dig deeper!
        4- In general, employer string distance should also be taken into account.
        4- if cities are different but employers are identical, then accept

        '''
#         Record.debug = True if   r1['N_first_name'] == r2['N_first_name'] == "MARKUS" and r1['N_last_name'] == r2['N_last_name'] == "AAKKO" else False
#         Record.debug = True  if r1['N_first_name'] != r2['N_first_name']  else False;  
        if Record.debug:
            print "_______________________________________________________________________________________________________________"
            print r1
            print r2
            # If states are the same,
        
        result = {'o':None,  # occupation
                  'e':None,  # employer
                  'n':None,  # name
                  'a':None,  # address
                  's':None,  # state
                  'c':None  # city
                  }
        
        
#         print self.pvalue(r1,r2)
        
        
        #=======================================================================
        # 
        #=======================================================================
        firstname1 = r1['N_first_name']
        firstname2 = r2['N_first_name']
        
#         if firstname1 == "ROSALIE" or firstname2 == "ROSALIE":
#             if self.compare_names(r1, r2):
#                 print "------>", r1['NAME'], "     |     ", r2['NAME']
             
        #===================================================================
        # 
        #===================================================================
    
        
        if r1['STATE'] == r2['STATE']:
            result['s'] = 1
            
            # If cities are the same or if zipcodes are the same
            # if zipcodes are the same, then treat as if cities are the same
            if r1['CITY'] == r2['CITY'] or self.compare_zipcodes(r1, r2) == 2:
                result['c'] = 1
            
                # If both have addresses
                if r1['N_address'] and r2['N_address']:
                    
                    # If addresses are the same
                    # TODO: need finer comparison of addresses
                    if r1['N_address'] == r2['N_address']:
                        result['a'] = 1
                        c_n = self.compare_names(r1, r2)
                        result['n'] = c_n
                        # TODO: if states are the same and addresses are the same
                        return (c_n >= 2), result
                    
                    # If addresses aren't the same
                    else:
                        # TODO: if states and cities are the same but addresses are different
                        # Accept if zipcodes are the same and at least one of the affiliations is clearly related and exact same name
                        (c_e, c_o, c_n, c_z) = self.compare_employers(r1, r2), self.compare_occupations(r1, r2), \
                                            self.compare_names(r1, r2), self.compare_zipcodes(r1, r2)
                        result['e'], result['o'], result['n'] = c_e, c_o, c_n

                        # Return Record.LARGE_NEGATIVE if middle names are different
                        if c_n < 0: return Record.LARGE_NEGATIVE, result

                        # What if zipcodes are different?
                        if c_z:
                            return self._verdict_addressN_zipY(c_n, c_e, c_o, method_id=method_id), result
                        else:
                            return self._verdict_addressN_zipN(c_n, c_e, c_o, method_id=method_id), result
                            
                        
                                      
                
                
                # if at least one doesn't have an address
                # TODO: if states and cities are the same, but at least one doesn't have an address
                else:
                    (c_e, c_o, c_n, c_z) = self.compare_employers(r1, r2), self.compare_occupations(r1, r2), \
                                            self.compare_names(r1, r2), self.compare_zipcodes(r1, r2)
                    result['e'], result['o'], result['n'] = c_e, c_o, c_n
                    # Return Record.LARGE_NEGATIVE if middle names are different
                    if c_n < 0: return Record.LARGE_NEGATIVE, result
                    
                    # if zip codes are the same, then relax the affiliation condition a bit
                    if c_z == 2:
                        return self._verdict_addressNA_zipY(c_n, c_e, c_o, method_id=method_id), result
                    else:
                        # If at least one doesn't have an address and zipcodes are different
                        # Accept if both the affiliations are clearly related and exact same name
                        return self._verdict_addressNA_zipN(c_n, c_e, c_o, method_id=method_id), result

            # If states are the same but cities are different
            else:
                # if cities are different
                # Accept if affiliations are clearly connected and names are exactly the same
                # TODO: check for timeline consistency
                result['c'], result['a'] = 0, 0
                (c_e, c_o, c_n, c_z) = self.compare_employers(r1, r2), self.compare_occupations(r1, r2), \
                                            self.compare_names(r1, r2), self.compare_zipcodes(r1, r2)
                
                result['e'], result['o'], result['n'] = c_e, c_o, c_n
                # Return LARGE_NEGATIVE if middle names are different
                if c_n < 0: return Record.LARGE_NEGATIVE, result
                return self._verdict_cityN(c_n, c_e, c_o, method_id=method_id), result
                

            
        # If states are DIFFERENT, 
        else:
            # TODO: If states are DIFFERENT, 
            # 1- Names should be very close.
            # 2- Occupations should be close
            # 3- Employers should be close
            # 4- Name token frequency should be taken into account. 
            # 5- Check for timeline consistency: Requires the Person objects
            
            result['s'], result['c'], result['a'] = 0, 0, 0
            (c_e, c_o, c_n, c_z) = self.compare_employers(r1, r2), self.compare_occupations(r1, r2), \
                                    self.compare_names(r1, r2), self.compare_zipcodes(r1, r2)
            result['e'], result['o'], result['n'] = c_e, c_o, c_n
            
            # Return LARGE_NEGATIVE if middle names are different
            if c_n < 0: return Record.LARGE_NEGATIVE, result
            
            # Actually, we will relax the conditions at this point. Cross-state consistency will be resolved on the Person level.
            return self._verdict_stateN(c_n, c_e, c_o, method_id=method_id), result
        return False, result
    
    
    
    '''Returns a number:
        0: zipcodes are different
        1: at least one doesn't have a zipcode
        2: zip codes are the same.'''
    def compare_zipcodes(self, r1, r2):
        if not r1['ZIP_CODE'] or not r2["ZIP_CODE"]:
            if Record.debug: print "One ZIP_CODE doesn't exist"
            return 1
        if r1['ZIP_CODE'][:5] == r2["ZIP_CODE"][:5]:
            return 2
        else:
            return 0
             
    
    
    def compare_occupations(self, r1, r2):
        '''
        Returns a number:
            0: they both exist but are unrelated
            1: at least one doesn't have the field
            2: connected in the affiliations network
            3: exactly the same but not "bad_identifier"
        '''
        try:
            occupation1 = r1['OCCUPATION']
        except KeyError:
            if Record.debug: print "no occupation field found"
            return 1
        
        try:
            occupation2 = r2['OCCUPATION']
        except KeyError:
            if Record.debug: print "no occupation field found"
            return 1
        
#         print occupation1, occupation2
        
        if bad_identifier(occupation1, type="occupation") or bad_identifier(occupation2, type="occupation"):
            return 1
        
        if occupation1 == occupation2:
            if Record.debug: print "occupations are the same" 
            return 3
        else:
            found_both = False
            for G_occupation in self.list_G_occupation:
                try:
                    ind1 = G_occupation.dict_string_2_ind[occupation1]
                    ind2 = G_occupation.dict_string_2_ind[occupation2]
                    found_both = True
                except KeyError:
                    if Record.debug: print "one of the occupations not found"
                    continue
                
                
                # Check if the occupation identifiers are adjacent in the affiliations graph
                if G_occupation.get_eid(ind1, ind2, directed=False, error=False) != -1:
                    if Record.debug: print "-------------", occupation1, occupation2
                    return 2
              
            
            
            # Either there was no graph that contained both
            # Or otherwise, they weren't adjacent in any of the graphs
            
            # If case 1
            if not found_both:
                return 1
            
            
            # Not adjacent. Check if strings are close
            if editdist.distance(occupation1, occupation2) < max(len(occupation1), len(occupation2)) * Record.occupation_str_tolerance:
                # Strings are close enough even though not linked on the affiliation graph
                return 2
            # Check if one's employer is mistakenly reported as its occupation
            elif occupation1 == r2['EMPLOYER'] or occupation2 == r1['EMPLOYER']:
                return 2
            else:
                # String distances not close enough
                if Record.debug: print "occupations are different and not adjacent"
                return 0
        
      
                
              
        
    
    
    def compare_employers(self, r1, r2):
        '''
        Returns a number:
            0: they both exist but are unrelated
            1: at least one doesn't have the field, or there is no affiliation graph that contains both
            2: connected in the affiliations network or employer1/2 == occupation2/1
            3: exactly the same, but not "bad_identifier"
        '''
        try:
            employer1 = r1['EMPLOYER']
        except KeyError:
            if Record.debug: print "no employer field found"
            return 1
        
        try:
            employer2 = r2['EMPLOYER']
        except KeyError:
            if Record.debug: print "no employer field found"
            return 1
        
        
#         print employer1,employer2
        
        if bad_identifier(employer1, type="employer") or bad_identifier(employer2, type="employer"):
            return 1



        if Record.debug: print "Both have employer field"
        
        
        if employer1 == employer2:
            if Record.debug: print "employers are the same"
            return 3
        else:
            found_both = False
            
            for G_employer in self.list_G_employer:
                try:
                    ind1 = G_employer.dict_string_2_ind[employer1]
                    ind2 = G_employer.dict_string_2_ind[employer2]
                    if Record.debug: print "found both"
                    found_both = True
                except KeyError:
                    if Record.debug: print "one of the employers not found"
                    continue
                
                # Check if the employer identifiers are adjacent in the affiliations graph
                if G_employer.get_eid(ind1, ind2, directed=False, error=False) != -1:
                    
                    # They are adjacent in at least one affiliation graph
                    if Record.debug: print "-------------", employer1, employer2
                    if Record.debug: print "ADJACENT"
                    return 2
                    
               
            # Either there was no graph that contained both
            # Or otherwise, they weren't adjacent in any of the graphs
            
            # If case 1
            if not found_both:
                return 1
        
            # Not adjacent. Check if strings are close
            if editdist.distance(employer1, employer2) < max(len(employer1), len(employer2)) * Record.employer_str_tolerance:
                # Strings are close enough even though not linked on the affiliation graph
                return 2
            
            # Check if one's employer is mistakenly reported as its occupation
            elif employer1 == r2['OCCUPATION'] or employer2 == r1['OCCUPATION']:
                return 2
            else:                
                # String distances not close enough
                if Record.debug: print "employers are not adjacent"
                return 0
            
            
            
            
            
            
    
    def compare_names(self, r1, r2):
        '''
        TODO: Returns a number:
        LARGENEGATIVE: middle names are different
        0: names are not related
        1: uncertain: at least one last name doesn't exist
        2: names are similar, but we can't verify if they are variants or misspelling. Let verdict functions decide. (useful when addresses are exactly identical)
        3: names are identical
        
        NOTE: you can't ever treat the output as a boolean in if statements: if (-1000) is True, if(0) is False. 
        '''
        identical = 3
        # if both have middlenames, they should be the same
        if r1['N_middle_name'] and r2['N_middle_name']:
            if r1['N_middle_name'][0] != r2['N_middle_name'][0]: 
                if Record.debug: print "middle names are different"
                identical = Record.LARGE_NEGATIVE
                return identical
        
#         # if 1 doesn't have a middle name but 2 does, then 2 is not the "parent" of 1
#         if not r1['N_middle_name'] and r2['N_middle_name']: 
#             identical = False
#             return identical
        
          
        # if one of the last names doesn't exist, 
        if not r1['N_last_name'] or not r2['N_last_name']: 
            if Record.debug:
                print "one of the last names doesn't exist"
            identical = 1 
            return identical      
        
        # Compute edit distance of last names
        distance = editdist.distance(r1['N_last_name'], r2['N_last_name'])

        # TODO: if both have last names take into account their frequencies
    
        if 0 < distance < 3 : 
            # get the tokens' frequencies.
            f1 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['LAST_NAME'][0], r1['N_last_name']))
            f2 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['LAST_NAME'][0], r2['N_last_name']))
            
            if f1 <= TokenData.RARE_FREQUENCY or f2 <= TokenData.RARE_FREQUENCY:
                # They are very similar and at least one is rare. Must be misspelling. Accept
                identical = 3
            else:
                # Don't reject out of hand. Simply report that they are different byt not too different.
                # Useful when records are otherwise very similar, e.g., have identical addresses. 
                identical = 2
                #return identical

        
        # Both have last names and they are too different. Reject
        else:
            if  distance >= 3:
                identical = False
                return identical

        # if first names don't overlap, then check if they are variants. If not, fail
        # if not any(i in dict1[2] for i in dict2[2]): identical = False
        firstname1 = r1['N_first_name']
        firstname2 = r2['N_first_name']
       
        
        # If first name saren't identical, check if one is a variant of the other. If not, see if either one is a misspelling, 
        # i.e., a name that doesn't appear in the dictionary of names.
        # If either is a misspelling, then if they're close enough, accept, else reject.
        # TODO: How do you define misspelling? Word frequency should also be part of it...

        if firstname1 != firstname2:            
            if Record.debug: print "First names are different"
            one_var_two = False
            two_var_one = False
            
            # Check if firstname1 is a variant of firstname2
            if firstname2 in self.tokendata.dict_name_variants:
                if firstname1  in self.tokendata.dict_name_variants[firstname2]: 
                    one_var_two = True
                else:
                    one_var_two = False

            # Check if firstname2 is a variant of firstname1
            if firstname1 in self.tokendata.dict_name_variants:
                if firstname2  in self.tokendata.dict_name_variants[firstname1]: 
                    two_var_one = True
                else:
                    two_var_one = False
            
            # If at least one of them is registered as the other's variant:
            if one_var_two or two_var_one:
                if Record.debug: 
                    print "first names ARE variants of each other"
                    print "VARIANTS:    ", firstname1, "-------------", firstname2
                identical = 3
            else:
                # If first names are different and neither is a variant of the other
                if Record.debug: print "first names aren't variants of each other"
                
                # If neither one is a misspelling, reject
                if firstname1  in self.tokendata.set_all_names and firstname2  in self.tokendata.set_all_names:
                    identical = False
                    return identical
                else:
                    # if at least one seems to be misspelled, (doesn't exists in list of all names)
                    
                    # If one of them is just the initial
                    if len(firstname1) == 1 or len(firstname2) == 1:
                        try:
                            if firstname1[0] == firstname2[0]:
                                identical = 2
                            else:
                                identical = False
                                return  identical
                        except IndexError:
                            # If one first name is empty
                            identical *= 1    
                        
                    # check edit distances.
                    # If the two names are different by only one edit and at least one of them is too rare, then we 
                    # consider them to be the same, only misspelled.
                    elif editdist.distance(firstname1, firstname2) < 2:
                        
                        # get the tokens' frequencies.
                        f1 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['FIRST_NAME'][0], firstname1))
                        f2 = self.tokendata.get_token_frequency((self.tokendata.token_identifiers['FIRST_NAME'][0], firstname2))
                        
                        # Accept if one of the names is very rare
                        if f1 <= TokenData.RARE_FREQUENCY or f2 <= TokenData.RARE_FREQUENCY:
                            identical *= 1
                        else:
                            identical = 2
                            # print "EQUIVALENT:    ", firstname1, "-------------", firstname2
                    else: identical = False
                        

        return identical


    def reformat_data(self):
        try:
            d = self['TRANSSCTION_DT']
        except KeyError:
            return 
        
        self['TRANSSCTION_DT'] = self.get_date

    def get_date(self):
        try:
            d = self['TRANSSCTION_DT']
        except KeyError:
            return ''
                
        return d[:4] + "-" + d[4:6] + "-" + d[6:]
        
        
                    
    def updateTokenData(self, newtokendata):
        ''' newtokendata is a TokenData object and is assumed to be a superset of self.tokendata. That is,
        every token or normalized token that exists in self.tokendata, also exists in newtokendata, but perhaps
        together with some additional tokens, and with different indexing.
        The main task here is to recompute the record's vector using the indexes of the newtokendata.
        Then, self.tokendata is replaced with newtokendata.'''
        pass
        vector = {}
        
        
        # translate self.vector
        for index_old in self.vector:
            index_new = newtokendata.token_2_index[self.tokendata.index_2_token[index_old]]
            vector[index_new] = 1
        
        self.vector = vector
        self.tokendata = newtokendata
        
                    
        
            

         
