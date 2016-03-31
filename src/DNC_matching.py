
# coding: utf-8

# In[147]:

import pandas as pd
from disambiguation.core import address as ad
from disambiguation.core import Database, Person
from disambiguation import config
import json
import sys
from disambiguation.core import editdist




# In[39]:

data = pd.read_csv('/nfs/home/navid/data/FEC/voter_registration/voterfile_sample100k_20150624.csv')


# In[41]:

def get_idm():

    idm = Database.IdentityManager('USA')
    idm.fetch_dict_id_2_identity()
    idm.fetch_dict_identity_2_id()
    idm.fetch_dict_identity_adjacency()
    return idm

# In[42]:

tokens = None


# In[144]:

class MatchFinder():
    def __init__(self, idm, tokens):
        '''
        @param idm: L{IdentityManager} instance to be used.
        @param tokens: Normalized tokens for all records.
        '''
        self.tablename = config.MySQL_table_usa_combined
        self.idm = idm
        self.tokens = tokens
        self.retriever_by_ID = Database.FecRetrieverByID(self.tablename)
        list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
        list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
        all_fields = list_tokenized_fields + list_auxiliary_fields
        
        self.where_clause_template = " WHERE MATCH(NAME) AGAINST('+{lastname} +{firstname}' IN BOOLEAN MODE) "
        # where_clause is left as a string template. must instantiate
        # using str.format()
        self.db = Database.FecRetriever(table_name='individual_contributions_v%s_MYISAM' % config.FEC_version_data,
                query_fields=all_fields,
                limit='',
                where_clause=" ")
        self.parser = ad.AddressParser()
        
        # A dict of identities checked.
        self.dict_persons ={}

        self.set_matched_pairs = None
        
    def __matches(self, record, data):
        '''
        Determine if an FEC record matches the data.
        '''
        if record['CITY'] != data['city']: return False
        if record['STATE'] != data['state']: return False
        
        return True
        
    def reject_query(self, data):
        '''
        Decide if the query should be skipped due to
        data problems. For instance, if the name fields
        are single letter, etc.
        '''
        if len(data['firstname']) < 3 or len(data['lastname']) < 3: return True
        if ' ' in data['firstname'] or ' ' in data['lastname']: return True
        if "'" in data['firstname'] or "'" in data['lastname']: return True
        if '"' in data['firstname'] or '"' in data['lastname']: return True
        
        
    def __normalize_address(self, address):
        '''
        Using addressparser, normalize the given address string.
        '''
        
        x = self.parser.parse_address(address)
        N_address = "{house} {street}".format(house=x.house_number, street=str(x.street).upper())
        return N_address
    
    
    def __compare_zipcodes(self, zip1, zip2):
        if not zip1 or not zip2: return False
        if zip1[:5] == zip2[:5]:
            return True
        else:
            return False
    
    def __match_zipcode(self, identity, data):
        '''
        Decide if the zipcode in data matches the zipcode of
        any of the records in identity.
        '''
        match = False
        if (identity, data['index']) in self.set_matched_pairs:
            return True
        target_zipcode = data['zipcode']
        list_ids = self.idm.get_ids(identity)
        if not list_ids: return False
        self.retriever_by_ID.retrieve(list_ids)
        list_records = self.retriever_by_ID.getRecords()
        p = Person.Person(list_records)
        for r in list_records:
            zipcode = r['ZIP_CODE']
            if not zipcode: continue
            if self.__compare_zipcodes(target_zipcode, zipcode):
                match = True
                break
                
        # Keep track of matched identities for
        # later further analysis.
        if match:
            self.dict_persons[identity] = (p, data)
            self.set_matched_pairs.add((identity, data['index']))

        return match
    
    def __match_address(self, identity, data):
        '''
        Decide if the address in data matches the address of
        any of the records in identity.
        '''
        match = False
        if (identity, data['index']) in self.set_matched_pairs:
            return True
        target_address = self.__normalize_address(data['address'])
        list_ids = self.idm.get_ids(identity)
        if not list_ids: return False
        self.retriever_by_ID.retrieve(list_ids)
        list_records = self.retriever_by_ID.getRecords()
        p = Person.Person(list_records)
        #person = Person.Person(list_records)
        for i,r in enumerate(list_records):
            address = r['CONTRIBUTOR_STREET_1']
            name = r['NAME']
            if not address: continue
            r_address = self.__normalize_address(address)
            if r_address == target_address:
                match = True
                break
            elif editdist.distance(r_address, target_address) < 2:
                match = True
                break
            elif editdist.distance(address, data['address']) < 2:
                match = True
                break
        # Keep track of matched identities for
        # later further analysis.
        if match:
            self.dict_persons[identity] = (p, data)
            self.set_matched_pairs.add((identity, data['index']))
            #print ' '*10 + name.ljust(30) + address.ljust(50) + r_address
            #print ' '*10 + data['name'].ljust(30) + data['address'].ljust(50) + target_address
            print ' ' * 15 +"==" + '_'.join([r['NAME'] for r in p.set_of_records])
            print ' ' * 15 +"==" + name
            print ' ' * 15 +"==" + data['name']

        sys.stdout.flush()
        return match
    
    def find_match(self, data, using='address'):
        '''
        Try to find a match in the FEC database for the specified record.
        @param using: which feature to match based on: "address" or "zipcode".
        @return: identity of the found match.
        
        '''
        if self.reject_query(data):
            return None
        
        self.db.where_clause = self.where_clause_template.format(lastname=data['lastname'], firstname = data['firstname'])
        result = []
        try:
            self.db.retrieve()
            list_records = self.db.getRecords()
            list_matches = [r for r in list_records if self.__matches(r, data)]
            list_matched_ids = [r.id for r in list_matches]
            list_matched_identities = list(set([self.idm.get_identity(rid) for rid in list_matched_ids]))

            if using == 'address':
                list_identities_address = [identity for identity in list_matched_identities  if self.__match_address(identity, data)]
                result = list(set(list_identities_address))
            elif using == 'zipcode':
                list_identities_zipcode = [identity for identity in list_matched_identities  if self.__match_zipcode(identity, data)]
                result = list(set(list_identities_zipcode))
             
        except Exception as e:
            print "ERROR: ", e
#             print "ERROR: ", self.db.query
            pass
        return result
    
        
        
def export_results(dict_results):
    '''
    Export the results of the matches to a csv file. An entry
    will include the index of the record in the voter file, the
    identity of the matched person in FEC data, zipcode, name and
    address of both the voter record and the FEC record.  
    '''
    columns = ['verdict', 'freq_zip', 'freq_state', 'identity', 'voter_index', 'feature', 'name', 'address', 'city', 'state', 'zipcode']
    list_rows = []
    for feature, dict_persons in dict_results.iteritems():
        for identity, result in dict_persons.iteritems():
            person, voter_data = result
            fec_name = person.get_dominant_attribute('NAME')
            fec_state = person.get_dominant_attribute('STATE')
            fec_city = person.get_dominant_attribute('CITY')
            fec_zipcode = person.get_dominant_attribute('ZIP_CODE')
            fec_address = person.get_dominant_attribute('CONTRIBUTOR_STREET_1')
            
            voter_name = voter_data['name']
            voter_state = voter_data['state']
            voter_city = voter_data['city']
            voter_zipcode = voter_data['zipcode']
            voter_address = voter_data['address']
            voter_index= voter_data['index']
            freq_zip = voter_data['freq_zip']
            freq_state = voter_data['freq_state']
            
            row1 = ['',freq_zip, freq_state, identity, voter_index, feature, voter_name, voter_address, voter_city, voter_state, voter_zipcode]
            row2 = ['', '', '', '', '', '', fec_name, fec_address, fec_city, fec_state, fec_zipcode]
            row3 = ['' for column in columns]
            list_rows.append(row1)
            list_rows.append(row2)
            list_rows.append(row3)
    dataframe = pd.DataFrame(list_rows, columns = columns)
    dataframe.to_csv('DNC_results.csv', sep = '|', header = columns, index = False)
    dataframe.to_html('DNC_results.html', index_names = False, index=False)
    
    
import pandas as pd
def get_dict_name_frequencies():
    '''
    Load the name frequencies from the additional DNC file.
    '''
    dict_freqs = {}    
    filename = '/nfs/home/navid/data/FEC/voter_registration/vf_name_frequencies.csv'
    data_nf = pd.read_csv(filename)
    for index, row in data_nf.iterrows():
        freq_zip = int(row['voters_in_zip5'])
        freq_state = int(row['voters_in_state'])
        person_id = str(row['personid'])
        dict_freqs[person_id] = (freq_zip, freq_state)
    return dict_freqs

# In[151]:e

def main_task(idm):
    dict_results = {}
    set_matched_pairs = set()
    dict_name_frequencies = get_dict_name_frequencies()
    for feature in [ 'address','zipcode']:
        matchfinder = MatchFinder(idm, tokens)
        matchfinder.set_matched_pairs = set_matched_pairs
        counter_match = 0

        with open('DNC_matches_using_{feature}.txt'.format(feature=feature), 'w') as outfile:
            for index, row in data.iterrows():
                index = int(index)
                d = {}
                d['firstname'] = str(row['first_name']).upper()
                d['lastname'] = str(row['last_name']).upper()
                d['name'] = d['lastname'] + ", " + d['firstname'] 
                d['address'] = str(row['reg_address_street1']).upper()
                d['city'] = str(row['reg_address_city']).upper()
                d['state'] = str(row['reg_address_state']).upper()
                d['index'] = str(row['personid'])

                try:
                    d['freq_zip'], d['freq_state'] = dict_name_frequencies[d['index']]
                except:
                    d['freq_zip'], d['freq_state'] = 0, 0
                
                try:
                    zipcode = str(int(row['reg_address_zip5']))
                    d['zipcode'] = zipcode
                except Exception as e:
                    print e
                    d['zipcode'] = None
            
                match = matchfinder.find_match(d, using=feature )

                if match:
                    print str(counter_match).ljust(10), str(index).ljust(10), '-----', d['name'], d['city'], d['state'], d['zipcode'], d['address']
                    print "-"*100
                    for s in match:
                        outfile.write(json.dumps([d['index'], match]) + "\n")
                    counter_match += 1
                

                #if counter_match == 20:break

        
        dict_results[feature] = matchfinder.dict_persons
                
    # Export results to a csv file for handcoding
    export_results(dict_results)

if __name__ == "__main__":
    idm = get_idm()
    main_task(idm)
