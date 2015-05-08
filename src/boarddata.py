'''
Created on May 7, 2015

@author: navid
'''


import numpy as np
import pandas as pd

from disambiguation.core import Record
from disambiguation.core import Project
from disambiguation.core import Tokenizer
from disambiguation.core import Disambiguator
from disambiguation.core import Database




def worker_get_similar_records_db(data):
    list_records = data['list_records']
    q = data['queue']
    
    db = Database.DatabaseManager()
    
    for r in list_records:
        print "="*70
        print "%s %s %s" %(r['N_first_name'], r['N_middle_name'], r['N_last_name'])
        print "="*70
        firstname = r['N_first_name']
        lastname = r['N_last_name']
        middlename = r['N_middle_name']
        query = 'SELECT * from individual_contributions WHERE NAME regexp "\b%s\b.*\b%s\b";' %(firstname,lastname)
        result = db.runQuery(query)
        print result

if __name__ == "__main__":
        
    
    state = 'boarddata'
    
    
    data = pd.read_csv('/home/navid/data/FEC/zubin/sample_data.csv')
    print data.columns
    
    list_of_records = []
    for x in data.iterrows():
        try:
            r = Record.Record()
            r.id = x[1]['directorid']
            r['NAME'] = x[1]["Director Name"].encode('ascii', 'ignore')
            r['EMPLOYER'] = x[1]["employment_Company Name"].encode('ascii', 'ignore')
            r['OCCUPATION'] =  x[1]["employment_Role"].encode('ascii', 'ignore')
            list_of_records.append(r)
        except:
            print "error"
    
    
    
    
    project = Project.Project(1)
    project.putData('state', state)
    project.putData('list_tokenized_fields',['NAME','EMPLOYER','OCCUPATION'])
    tokenizer = Tokenizer.Tokenizer()
    project.tokenizer = tokenizer
    tokenizer.project = project
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(project['list_tokenized_fields'])
    
    
    print "Tokenizing records..."
    tokenizer.tokenize()
    
    list_of_records = tokenizer.getRecords()
    
    
    
    
    
    worker_get_similar_records_db({'list_records': list_of_records,'queue':None})
        
    quit()
    
    
    
    
    
    tokendata = tokenizer.tokens
        
       
    # dimension of input vectors
    dim = tokendata.no_of_tokens
    
    D = Disambiguator.Disambiguator(list_of_records, dim, matching_mode="strict_address", num_procs=1)
    project.D = D
    D.project = project
    D.tokenizer = tokenizer
    
    # desired dimension (length) of hashes
    hash_dim = 60
    project.putData('hash_dim' , str(hash_dim))
    
    # In D, how many neighbors to examine?
    B = 40
    
    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))
    
    # compute the hashes
    print "Computing LSH hashes for state %s using TokenizerClass: %s" % (state, tokenizer.__class__.__name__) 
    D.compute_hashes(hash_dim, 1)
    print "Done computing LSH hashes."
    
