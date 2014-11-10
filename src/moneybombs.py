from Database import FecRetriever
from main_general import *
from geocoding import *
import pandas as pd
import matplotlib.pyplot as plt
import re
import pickle
from datetime import datetime
from nameparser import HumanName






def get_candidates():

    retriever = FecRetriever(table_name='candidate_master',
                      query_fields=['id','CAND_ID','CAND_NAME','CAND_OFFICE_ST','CAND_PTY_AFFILIATION'],
                      limit=(0,100000000),
                      list_order_by='',
                      where_clause=" WHERE CAND_OFFICE in ('H','S') ")
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
    
    list_zipcodes = " ('02108', '02109', '02110', '02111', '02112', '02113', '02114', '02115', '02116', '02117', '02118', '02119', '02120', '02121', '02122', '02123', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132', '02133', '02134', '02135', '02136', '02137', '02163', '02196', '02199', '02201', '02203', '02204', '02205', '02206', '02210', '02211', '02212', '02215', '02217', '02222', '02228', '02241', '02266', '02283', '02284', '02293', '02295', '02297', '02298', '02467') "

    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(1,10000000),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      #where_clause=' WHERE CITY IN '+ list_cities  +' and STATE="MA" AND TRANSACTION_DT BETWEEN "2014-01-01" and "2014-12-30" '
                      where_clause=' WHERE ((CITY IN '+ list_cities  +' ) OR (ZIP_CODE IN ' + list_zipcodes  + ')) AND STATE="MA" AND TRANSACTION_DT BETWEEN "2014-01-01" and "2014-12-30" ',

                      )
    retriever.retrieve()
    
    list_of_records = retriever.getRecords()
    return list_of_records






def get_transactions_PAS():
    #list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    
    all_fields = list_tokenized_fields + list_auxiliary_fields 


    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]
    
    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]    
    
    
    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    
    field_2_index = { s:all_fields.index(s) for s in all_fields}
    
    table_name = "contributions_to_candidates"
        
    list_cities = ' ("BOSTON","FENWAY","BROOKLINE","CAMBRIDGE","NEWTON","BRIGHTON","SOUTH BOSTON","BELMONT","ARLINGTON","WALTHAM","SOMERVILLE","JAMAICA PLAIN","OAK HILL","NEEDHAM", "NORTH WALTHAM","WATERTOWN","DORCHESTER","MATTAPAN","MEDFORD","DEDHAM","WESTON","MILTON") '
    #list_cities = ' ("BOSTON","CAMBRIDGE","SOMERVILLE") '
    
    list_zipcodes = " ('02108', '02109', '02110', '02111', '02112', '02113', '02114', '02115', '02116', '02117', '02118', '02119', '02120', '02121', '02122', '02123', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132', '02133', '02134', '02135', '02136', '02137', '02163', '02196', '02199', '02201', '02203', '02204', '02205', '02206', '02210', '02211', '02212', '02215', '02217', '02222', '02228', '02241', '02266', '02283', '02284', '02293', '02295', '02297', '02298', '02467') "

    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(1,10000000),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      #where_clause=' WHERE CITY IN '+ list_cities  +' and STATE="MA" AND TRANSACTION_DT BETWEEN "2014-01-01" and "2014-12-30" '
                      where_clause=' WHERE ((CITY IN '+ list_cities  +' ) OR (ZIP_CODE IN ' + list_zipcodes  + ')) AND STATE="MA" AND TRANSACTION_DT BETWEEN "2014-01-01" and "2014-12-30" ',

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



def get_full_address(record):
    '''
    add city and state to address
    '''
    if not record['CONTRIBUTOR_STREET_1'] : return None
    return str(record['CONTRIBUTOR_STREET_1']) + ", " + str(record['CITY']) + ", MA"


def get_distinct_addresses(list_of_records):
    set_addresses = set()
    for record in list_of_records:
        if record['CONTRIBUTOR_STREET_1']:
            if re.match(r'.*CHAPEL.*',record['CONTRIBUTOR_STREET_1']): print record
            date = record['TRANSACTION_DT']
            if (date >= "20140101") and (date <= "20141230"):
                #set_addresses.add(record['N_address'])        
                set_addresses.add(get_full_address(record))        
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



def get_dict_addresses(data):
    '''
    input: a dataframe with columns address, lat,lng    
    output: a dictionary {address: (lat,lng)}
    '''
    dict_addresses = {}
    for index, row in data.iterrows():
        dict_addresses[row['address']] = (row['lat'],row['lng'])
    return dict_addresses
        

def normalize_name(name):
    s1 = re.sub(r'\.|[0-9]+', '', name)
    s1 = re.sub(r'(\bESQ\b)|(\bENG\b)|(\bINC\b)|(\bLLC\b)|(\bLLP\b)|(\bMRS\b)|(\bPHD\b)|(\bSEN\b)', '', s1)
    s1 = re.sub(r'(\bDR\b)|(\bII\b)|(\bIII\b)|(\bIV\b)|(\bJR\b)|(\bMD\b)|(\bMR\b)|(\bMS\b)|(\bMISS\b)|(\bSR\b)|(\bSGT\b)|(\bDC\b)|(\bREV\b)|(\bFR\b)', '', s1)
    s1 = re.sub(r'\.', '', s1)

    name = HumanName(s1)

    last = name.last.upper()
    first = name.first.upper()
    return first,last


def plot_coordinates(data):
    lng = data['lng'].values.tolist()
    lat = data['lat'].values.tolist()
    plt.plot(lng,lat,'o')
    plt.plot([71.0636],[42.3581],'ro')
    plt.savefig('tmp.png')
    #plt.show()



def disambiguate(do_new = False):
    if not do_new:
        try:
            f = open('moneybombs_project.pickle', 'r')
            project = pickle.load(f)
            f.close()    
            return project
        except:
            pass
    list_cities = ' ("BOSTON","FENWAY","BROOKLINE","CAMBRIDGE","NEWTON","BRIGHTON","SOUTH BOSTON","BELMONT","ARLINGTON","WALTHAM","SOMERVILLE","JAMAICA PLAIN","OAK HILL","NEEDHAM", "NORTH WALTHAM","WATERTOWN","DORCHESTER","MATTAPAN","MEDFORD","DEDHAM","WESTON","MILTON") '
 
    list_zipcodes = " ('02108', '02109', '02110', '02111', '02112', '02113', '02114', '02115', '02116', '02117', '02118', '02119', '02120', '02121', '02122', '02123', '02124', '02125', '02126', '02127', '02128', '02129', '02130', '02131', '02132', '02133', '02134', '02135', '02136', '02137', '02163', '02196', '02199', '02201', '02203', '02204', '02205', '02206', '02210', '02211', '02212', '02215', '02217', '02222', '02228', '02241', '02266', '02283', '02284', '02293', '02295', '02297', '02298', '02467') "

    project = disambiguate_main('massachusetts',
                       record_limit=(0,100000),
                       logstats=False,
                       whereclause=' WHERE ((CITY IN '+ list_cities  +' ) OR (ZIP_CODE IN ' + list_zipcodes  + ')) AND STATE="MA" AND TRANSACTION_DT BETWEEN "2014-01-01" and "2014-12-30" AND ENTITY_TP="IND" ',
                       num_procs=10,
                       percent_employers = 5,
                       percent_occupations = 5)

    project.D.tokenizer.tokenize_functions = None 
    project.D.tokenizer.normalize_functions = None 
    project.D.project = None

    f = open('moneybombs_project.pickle', 'w')
    pickle.dump(project, f)
    f.close()    
    return project







def infer_addresses(project):
    '''
    Using a disambiguation project, infer the missing addresses.
    '''

    def get_compat_records(record, set_of_records):
        '''
        for a given record, find list of other records within list_of_records 
        that have the same CITY and ZIP_CODE and already have an address.
        '''
        list_compat_records = []
        for r in set_of_records:
            if r is record: continue
            if r['CITY'] == record['CITY'] and r['ZIP_CODE'][:5] == record['ZIP_CODE'][:5] and r['CONTRIBUTOR_STREET_1']:
                list_compat_records.append(r)

        return list_compat_records            




    D = project.D
    print len(project.D.list_of_records), len([1 for r in project.D.list_of_records if r['CONTRIBUTOR_STREET_1']])
    counter_new_address = 0
    for person_id, person in D.town.dict_persons.iteritems():

        set_of_records = person.set_of_records
        n = len(set_of_records)
        n_with_addresses = len([r for r in set_of_records if r['CONTRIBUTOR_STREET_1']])

        # If all have addresses, continue
        if n == n_with_addresses: continue

        if n_with_addresses == 0: continue
        
        

        for r in set_of_records:
            #r['CONTRIBUTOR_STREET_1'] = 'asdf'
            address = r['CONTRIBUTOR_STREET_1']
            address_old = address

            if address: continue

            # List of records in set_of_records that have an address, and have the same CITY and ZIP as r.
            list_compat_records = get_compat_records(r,set_of_records)
            if not list_compat_records: continue 

            # Dict of addresses with frequencies
            dict_distinct_addresses = {}
            for r2 in list_compat_records:
                address = r2['CONTRIBUTOR_STREET_1']
                if address:
                    try:
                        dict_distinct_addresses[address] += 1
                    except KeyError:
                        dict_distinct_addresses[address] = 1

            
            list_distinct_addresses = dict_distinct_addresses.keys() 
            list_distinct_addresses.sort(key = lambda address: dict_distinct_addresses[address])
            n_distinct_addresses = len(list_distinct_addresses)

            if n_distinct_addresses == 0: continue

            if n_distinct_addresses == 1:
                address = list_distinct_addresses[0]
                r['CONTRIBUTOR_STREET_1'] = address
                print "chosen address: ", address
                counter_new_address += 1
                continue


            # add r to list_compat_records, sort by date, and register r's index
            list_compat_records.append(r)
            list_compat_records.sort(key = lambda r: r['TRANSACTION_DT'])
            index = list_compat_records.index(r)
                        
            address_next = None
            address_previous = None

            try:
                r_next = list_compat_records[inex+1]
                r_previous = list_compat_records[index-1]
                address_next = r_next['CONTRIBUTOR_STREET_1']
                address_pervious = r_previous['CONTRIBUTOR_STREET_1']
            except:
                pass
            
            # If both before and after have addresses, pick the more frequent one
            if address_next and address_previous:
                address = max(address_next,address_previous, key = lambda address: dict_distinct_addresses[address])
            elif address_next: 
                address = address_next
            elif address_previous:
                address = address_previous
            else:
                address = max(list_distinct_addresses, key = lambda address: dict_distinct_addresses[address])

            counter_new_address += 1

            print "Chosen address: ", address  
            r['CONTRIBUTOR_STREET_1'] = address

        
#    for person_id, person in D.town.dict_persons.iteritems():
#        print person
    print counter_new_address
    print len(project.D.list_of_records), len([1 for r in project.D.list_of_records if r['CONTRIBUTOR_STREET_1']])
    project.save_data_textual(file_label = "with_addresses")



def get_days_since(str1,str2):
    '''
    take two date strings like "20140123" and return the number of 
    days between them.
    '''
    date_format = "%Y%m%d"
    a = datetime.strptime(str1, date_format)
    b = datetime.strptime(str2, date_format)
    delta = b - a
    return delta.days




def print_stats(list_of_records):
    '''
    print some statistics about the list of records.
    '''
    n_records = len(list_of_records)
    n_addresses = len([1 for r in list_of_records if r['CONTRIBUTOR_STREET_1']])
    n_distinct_addresses = len(get_distinct_addresses(list_of_records))
    print "number of records: ", n_records
    print "number of records with address: ", n_addresses
    print "number of distinct addresses: ", n_distinct_addresses






#list_of_records = get_transactions()
#print_stats(list_of_records)
#dl_coordinates(list_of_records)
#quit()



''' Get transactions from the contributions_to_candidates table (PAS files)'''
#list_of_records = get_transactions_PAS()
#print len(list_of_records)
#quit()




#plot_coordinates(load_coordinates())
#quit()

project = disambiguate( do_new = False)






# Updates records inside the project with inferred addresses
infer_addresses(project)
list_of_records = project.D.list_of_records
list_of_records.sort(key = lambda r : r['TRANSACTION_DT'])
#list_of_records.sort(key = lambda r : abs(r['TRANSACTION_AMT']))
print [r['TRANSACTION_AMT'] for r in list_of_records][:100]
print len([1 for r in list_of_records if r['TRANSACTION_AMT'] >200])





#list_of_records = get_transactions()
#dl_coordinates(list_of_recorMTE


print len(list_of_records)

list_candidates = get_candidates()
list_committees = get_committees()
dict_addresses = get_dict_addresses(load_coordinates())

# dict { CMTE_ID: 
#            {'CAND_ID': '',
#             'CMTE_PTY_AFFILIATION': ''}
#        }
dict_cmte = {}
dict_parties = {"REP":"R","DEM":"D"}

dict_cand_name = {}
dict_cand_party = {}
dict_cand_state = {}
                               

for r in list_candidates:
    dict_cand_name[r['CAND_ID']] = r['CAND_NAME']
    dict_cand_party[r['CAND_ID']] = r['CAND_PTY_AFFILIATION']
    dict_cand_state[r['CAND_ID']] = r['CAND_OFFICE_ST']
   
dict_candidate_amounts = {CAND_ID:{'timeline': [0 for i in range(274)] ,
                                   'state': None,
                                   'party':None,
                                   } for CAND_ID in dict_cand_name } 
 



counter = 0
for r in list_committees:
    if r['CAND_ID']:
        counter += 1
        #print counter,'\t', r['CAND_ID'], r['CMTE_ID'], dict_cand_name[r['CAND_ID']]
        dict_cmte[r['CMTE_ID']] =  {'CAND_ID':r['CAND_ID'], 'CMTE_PTY_AFFILIATION':r['CMTE_PTY_AFFILIATION'] }
    else:
        dict_cmte[r['CMTE_ID']] =  {'CAND_ID':None, 'CMTE_PTY_AFFILIATION':r['CMTE_PTY_AFFILIATION'] }
                


counter = 0
address_counter = 0
counter_campaign_cont = 0
counter_both = 0
counter_negative_amount =0

data = []
data_candidates = []
data_candidates_columns = ['recipient_ext_id','recipient_LastName','recipient_FirstName','recipient_MiddleName','recipient_state','recipient_party'] + range(1,275)

for r in list_of_records:
    counter += 1

    ADDRESS = None
    CAND_NAME = None
    AMNT = None
    AMNT = int(r['TRANSACTION_AMT'])
    if int(AMNT) < 0 : counter_negative_amount += 1 
    if AMNT < 200: continue

    CMTE_PTY_AFFILIATION = None

    try:
        CAND_ID = dict_cmte[r['CMTE_ID']]['CAND_ID']
    except:
        CAND_ID = None


    try:
        CAND_NAME = dict_cand_name[dict_cmte[r['CMTE_ID']]['CAND_ID']]
        counter_campaign_cont += 1
    except KeyError:
        pass

    try:
        CMTE_PTY_AFFILIATION = dict_cmte[r['CMTE_ID']]['CMTE_PTY_AFFILIATION']        
    except KeyError:
        CMTE_PTY_AFFILIATION = None
    
    try:
        CAND_PTY_AFFILIATION = dict_cand_party[dict_cmte[r['CMTE_ID']]['CAND_ID']] 
    except KeyError:
        CAND_PTY_AFFILIATION = None





    try:
        PARTY = dict_parties[CMTE_PTY_AFFILIATION]
    except:
        print "BAD PARTY: ",CMTE_PTY_AFFILIATION 
        PARTY=""

    if not PARTY:
        try:
            PARTY = dict_parties[CAND_PTY_AFFILIATION]
        except:
            print "BAD PARTY: ",CAND_PTY_AFFILIATION 
            PARTY=""
       
    
    DATE = r['TRANSACTION_DT']
    ADDRESS = get_full_address(r) 
    if ADDRESS:
        coords = dict_addresses[ADDRESS]
        address_counter += 1
    else: 
        coords = ""

    if ADDRESS and CAND_NAME:
        counter_both += 1

    if ADDRESS:
        day_of_year = get_days_since('20140101',DATE)+1
        try:
            CAND_FIRST_NAME,CAND_LAST_NAME = normalize_name(CAND_NAME)
            CAND_NAME_NORMALIZED = CAND_LAST_NAME+", "+CAND_FIRST_NAME
        except: 
            CAND_NAME_NORMALIZED = CAND_NAME

        row = [day_of_year, coords[0],coords[1],AMNT, CAND_NAME_NORMALIZED ,PARTY]
        data.append(row)
        print row


    if CAND_ID:
        try:
            dict_candidate_amounts[CAND_ID]['timeline'][day_of_year] += AMNT
            dict_candidate_amounts[CAND_ID]['state'] = dict_cand_state[CAND_ID]
            dict_candidate_amounts[CAND_ID]['party'] = PARTY
        except:
            pass
        

df = pd.DataFrame(data, columns = ['day_of_year','lat','lon','amount','recipient_name','party'])
print df

print "Number of records with an address: ", address_counter
print "Number of transactions to an official camopaign: ", counter_campaign_cont    
print "Number of transactions with CAND_NAME and ADDRESS: ", counter_both
print "Number of transactions with negative amount: ", counter_negative_amount 




df.to_csv('boston_greater_congressional.csv',index_label='ID')










for CAND_ID in dict_candidate_amounts:
    CAND_NAME = dict_cand_name[CAND_ID]
    CAND_FIRST_NAME,CAND_LAST_NAME = normalize_name(CAND_NAME)
    STATE = dict_candidate_amounts[CAND_ID]['state']
    PARTY = dict_candidate_amounts[CAND_ID]['party'] 
    if sum(dict_candidate_amounts[CAND_ID]['timeline']) ==0: continue
    data_candidates.append([CAND_ID,CAND_LAST_NAME,CAND_FIRST_NAME,None,STATE,PARTY] + dict_candidate_amounts[CAND_ID]['timeline'])

data_candidates.sort(key = lambda row: sum(row[6:]), reverse=True)

df = pd.DataFrame(data_candidates, columns = data_candidates_columns) 
df.to_csv('boston_amounts_congressional.csv',index=False)







