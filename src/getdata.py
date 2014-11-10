from Database import FecRetriever
from main_general import *
#from geocoding import *
import pandas as pd
import matplotlib.pyplot as plt





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
                      query_fields=['id','CMTE_ID','CAND_ID','CMTE_PTY_AFFILIATION'],
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
        
    list_cities = ' ("BOSTON","FENWAY","BROOKLINE","CAMBRIDGE","NEWTON","BRIGHTON","SOUTH BOSTON","BELMONT","ARLINGTON","WALTHAM","SOMERVILLE","JAMAICA PLAIN","OAK HILL","NEEDHAM", "NORTH WALTHAM","WATERTOWN","DORCHESTER","MATTAPAN","MEDFORD","DEDHAM","WESTON","MILTON") '
    #list_cities = ' ("BOSTON","CAMBRIDGE","SOMERVILLE") '

    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(1,10000000),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                       where_clause=' WHERE CITY IN '+ list_cities  +' and STATE="MA" AND TRANSACTION_DT BETWEEN "2014-01-01" and "2014-12-01" '

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
            if (date > "20140101") and (date < "20141201"):
                #set_addresses.add(record['N_address'])        
                set_addresses.add(record['CONTRIBUTOR_STREET_1'] + ", " + record['CITY'] + ", MA")        
    return set_addresses




def dl_coordinates(list_of_records):
    '''
    Download the geographic coordinates of all distinct addresses into a file coordinates.txt
    '''

    print "Number of records:", len(list_of_records)
    print "Number of records with address: ", len([1 for r in list_of_records if r['CONTRIBUTOR_STREET_1']])

    set_addresses = get_distinct_addresses(list_of_records)
    print "Number of distinct addresses: ", len(set_addresses)

    set_addresses_old = get_addresses_old()
    print "Number of addresses already coded: ", len(set_addresses_old)

    set_addresses_new = set_addresses.difference(set_addresses_old)




    print "Number of addresses to encode: ", len(set_addresses_new)
    #quit()

    print "Press enter to continue..."
    raw_input()


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


def load_coordinates():
    '''
    Load the data from coordinates.txt
    returns a DataFrame
    '''
    data = pd.read_csv("coordinates.txt",sep="\t",quotechar='"',names=['address','lat','lng'])
    return data


def plot_coordinates(data):
    lng = data['lng'].values.tolist()
    lat = data['lat'].values.tolist()
    plt.plot(lng,lat,'o')
    plt.plot([-71.0636],[42.3581],'ro')

    #plt.savefig('tmp.png')
    plt.show()

plot_coordinates(load_coordinates())
quit()



list_of_records = get_transactions()
dl_coordinates(list_of_records)

quit()
print len(list_of_records)
quit()

list_candidates = get_candidates()
list_committees = get_committees()

dict_cmte_cand = {}
dict_cand_name = {}

for r in list_candidates:
    dict_cand_name[r['CAND_ID']] = r['CAND_NAME']
   




counter = 0
for r in list_committees:
    if r['CAND_ID']:
        counter += 1
        #print counter,'\t', r['CAND_ID'], r['CMTE_ID'], dict_cand_name[r['CAND_ID']]
        dict_cmte_cand[r['CMTE_ID']] = r['CAND_ID']
        

list_of_records.sort(key = lambda r : r['TRANSACTION_DT'])

counter = 0
for r in list_of_records:
    try:
        counter += 1
        CAND_NAME = dict_cmte_cand[r['CMTE_ID']] 
        DATE = r['TRANSACTION_DT']
        print counter, DATE, CAND_NAME
    except KeyError:
        pass
    








