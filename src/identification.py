'''
Created on Oct 12, 2014

@author: navid
'''

import hashlib
from Database import FecRetriever

def collisionTest():
    retriever = FecRetriever(table_name='individual_contributions',
                      query_fields="*",
                      limit=(0, 100000000),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE", "CMTE_ID"],
                      where_clause=" ")
    print retriever.getQuery()
    
    
    retriever.retrieve()
    list_of_records = retriever.getRecords()

    

collisionTest()


# h = hashlib.md5('navid')
# print h.hexdigest()
