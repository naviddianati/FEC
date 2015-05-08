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
from disambiguation.core import utils




def worker_get_similar_records_db(r):
    
    db = Database.DatabaseManager()

    list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    all_fields = list_tokenized_fields + list_auxiliary_fields

    firstname = r['N_first_name']
    lastname = r['N_last_name']
    middlename = r['N_middle_name']

    db = Database.FecRetriever(table_name='individual_contributions',
                      query_fields=all_fields,
                      limit='',
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      where_clause=" WHERE NAME REGEXP '[[:<:]]%s.*[[:<:]]%s'" %(lastname,firstname) 
    )
    db.retrieve()
    print "records retrieved"
    list_records = db.getRecords() 
 
    print "saving to file"
    with open(firstname + "-" + lastname + ".txt", 'w') as f:
        f.write("="*70 + "\n")
        f.write("%s %s %s\n" %(r['N_first_name'], r['N_middle_name'], r['N_last_name']))
        f.write("="*70 + "\n")
        for record in list_records:
            f.write('  '.join([str(record[field]) for field in all_fields]) + "\n")
            

def get_similar_records_multiproc(list_records):
    pool = utils.multiprocessing.Pool(1)
    pool.map(worker_get_similar_records_db, list_records)


if __name__ == "__main__":
        
    
    state = 'boarddata'
    
    
    data = pd.read_csv('/nfs/home/navid/data/FEC/zubin/sample_data.csv')
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
    
    
    get_similar_records_multiproc(list_of_records) 
    
    
        
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
    
