from Database import FecRetriever
from main_general import *
from geocoding import *
import pandas as pd





def get_candidates():

    retriever = FecRetriever(table_name='candidate_master',
                      query_fields=['id','CAND_ID','CAND_NAME','CAND_ST','CAND_PTY_AFFILIATION'],
                      limit=(0,100000000),
                      list_order_by='',
                      where_clause="")
    retriever.retrieve()
    
    list_of_records = retriever.getRecords()
    return list_of_records


def get_committees():
    ''' NOT COMPLETE'''

    retriever = FecRetriever(table_name='committee_master',
                      query_fields=['id','CAND_ID','CAND_NAME','CAND_ST','CAND_PTY_AFFILIATION'],
                      limit=(0,100000000),
                      list_order_by='',
                      where_clause="")
    retriever.retrieve()
    
    list_of_records = retriever.getRecords()
    return list_of_records



def get_transactions():
    list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    
    all_fields = list_tokenized_fields + list_auxiliary_fields 


    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]
    
    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]    
    
    
    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    
    field_2_index = { s:all_fields.index(s) for s in all_fields}
    
    table_name = "massachusetts_combined"
    
    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(1,10000000),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      where_clause=' WHERE CITY IN ("BOSTON","CAMBRIDGE","SUMMERVILLE") and STATE="MA" AND TRANSACTION_DT BETWEEN "2012-01-01" and "2012-12-01" '
                      )
    retriever.retrieve()
    
    list_of_records = retriever.getRecords()
    return list_of_records




def tokenize(list_of_records):
    list_tokenized_fields = ['TRANSACTION_DT', 'CONTRIBUTOR_STREET_1']
    tokenizer = TokenizerNgram()
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(list_tokenized_fields)
    tokenizer.tokenize()
    return tokenizer.getRecords()
 

def get_addresses_old():
    '''
    return the set of addresses already found in the file coordinates.txt        
    '''
    data = pd.read_csv("coordinates.txt",sep="\t",quotechar='"',names=['address','lat','lng'])
    return set(data['address'].values.tolist())


def get_distinct_addresses(list_of_records):
    set_addresses = set()
    for record in list_of_records:
        if record['CONTRIBUTOR_STREET_1']:
            date = record['TRANSACTION_DT']
            if (date > "20120101") and (date < "20121201"):
                #set_addresses.add(record['N_address'])        
                set_addresses.add(record['CONTRIBUTOR_STREET_1'] + ", " + record['CITY'] + ", MA")        
    return set_addresses


#list_candidates = get_candidates()
list_of_records = get_transactions()
#list_of_records = tokenize(list_of_records)

'''project = disambiguate_main('massachusetts',
                         record_limit=(0, 5000000),
                         method_id="thorough",
                         logstats=False, 
                         whereclause=' WHERE CITY IN ("BOSTON","CAMBRIDGE","SUMMERVILLE") and STATE="MA" ',
                         num_procs=12,
                         percent_employers=5,
                         percent_occupations=5)

D = project.D'''

set_addresses = get_distinct_addresses(list_of_records)
set_addresses_old = get_addresses_old()
set_addresses_new = set_addresses.difference(set_addresses_old)


print "Number of records:", len(list_of_records)
print "Number of distinct addresses: ", len(set_addresses)
print "Number of records with address: ", len([1 for r in list_of_records if r['CONTRIBUTOR_STREET_1']])
print "Number of addresses already coded: ", len(set_addresses_old)


print len(set_addresses_new)
#quit()

f1 = open("coordinates.txt",'a',0)
f2 = open("coordinates-errors.txt",'w',0)

counter = 0
for s in set_addresses_new: 
    #if counter > 10: break
    print counter,"/",len(set_addresses_new)
    address = s
    #print address
    time.sleep(0.2)
    try:
        coords =    get_coords(address)
        f1.write('"%s"\t"%s"\t"%s"\n' %(address, coords['lat'],coords['lng']))
        counter += 1
    except Exception as e:
        f2.write(address+" "+str(e)+"\n")
        print str(e)
        
    
f1.close()
f2.close()

