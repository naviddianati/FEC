'''
Created on Oct 12, 2014

@author: navid

This script loads the specified columns for all records, concatenates 
the columns for each record into a string, and determines whether these
strings are unique.
'''

import hashlib
from Database import FecRetriever


def getHash(string):
    #h = hashlib.sha256(string)
    h = hashlib.md5(string)
    return string
    return h.hexdigest()

def toString(record):
    list_fields = ['NAME','TRANSACTION_DT','CMTE_ID','TRAN_ID','ZIP_CODE','CITY','STATE','TRANSACTION_AMT','OCCUPATION','EMPLOYER','IMAGE_NUM','OTHER_ID','FILE_NUM','SUB_ID']
    data = [str(record[field]) if record[field] else 'None' for field in list_fields]
    return ''.join(data)


def collisionTest():
    query_fields = ['id','NAME','TRANSACTION_DT','CMTE_ID','TRAN_ID','ZIP_CODE','CITY','STATE','TRANSACTION_AMT','OCCUPATION','EMPLOYER','IMAGE_NUM','OTHER_ID','FILE_NUM','SUB_ID']
    retriever = FecRetriever(
                      #table_name='individual_contributions',
                      #table_name='delaware_combined',
                      table_name='individual_contributions',
                      query_fields=query_fields,
                      limit=(0, 10000000),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE", "CMTE_ID"],
                      #list_order_by=[],
                      where_clause=" ")
    query =  retriever.getQuery()
    #retriever.runQuery("SET SESSION max_heap_table_size=30000000000;SET SESSION tmp_table_size=30000000000;")
    #retriever.runQuery("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;")
    
    print "Retrieving data from database..."
    retriever.retrieve()
    quit() 
    retriever.runQuery("COMMIT;")

    list_of_records = retriever.getRecords()

    print "Records loaded."
    print "Press enter to continue..."
    #raw_input()
    
    set_hashes = set()
    for record in list_of_records:
        string = toString(record)
        myhash = getHash(string)
        #print myhash
        set_hashes.add(myhash)
    
    print len(set_hashes)
    print len(list_of_records)
    #raw_input()
    
    print "Done..."
    print "Press enter to continue..."    

collisionTest()





# perform a test to measure the number of hash collisions that will occur in the individual_contributions table
collisionTest()
