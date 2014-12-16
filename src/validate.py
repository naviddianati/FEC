from Database import DatabaseManager
from Database import FecRetriever
import random
import pandas as pd





css_code = "table{border-collapse:collapse;\
            padding:5px;\
            font-family:sans;\
#                     width:100%;\
            font-size:10px;\
            border:dotted thin #efefef;}\
            td{padding:0px;\
            border:dotted thin #efefef;}\
            .significant{background:#ececec; display:inline-block;padding:5px;width:100%;height:100%} "

def get_identities_from_db():
    '''Export the calculated identities of the records to a database table called "identities".'''
    db_manager = DatabaseManager()    

    
    result = db_manager.runQuery('SELECT id,identity FROM identities')
    db_manager.connection.commit()
    db_manager.connection.close()
    return result





def load_records(param_state = "newyork", whereclause = ' ', record_limit = (0,5000000)):

    record_start = record_limit[0]
    record_no = record_limit[1]
    table_name = param_state + "_combined"

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
    
    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      where_clause=whereclause
                      )
    retriever.retrieve()
    
    list_of_records = retriever.getRecords()
    return list_of_records


if __name__ == "__main__":

    N_names = 100    
    N_records = 6000000
    tablename = "newyork_combined"

    list_ids_full =  get_identities_from_db()
    
    # Maps id to identity
    dict_ids_full = {x[0]: x[1] for x in list_ids_full}

    # dictionary mapping identity to list of associated ids
    dict_identities_full = {}
    for record_id, identity in dict_ids_full.iteritems():
        try:
            dict_identities_full[identity].append(record_id)
        except KeyError:
            dict_identities_full[identity] = [record_id]



    list_of_records = load_records(record_limit=(0,N_records))
    N_records = len(list_of_records)
    dict_of_records = {r['id']:r for r in list_of_records}

    # print the retrieved records
    #for record_id,identity in list_ids_full:
    #    print record_id, dict_of_records[int(record_id)]
    
    
    # Select N_names random names from the retrieved
    # records.
    set_names = set()
    while len(set_names) < N_names:
        n = random.randint(0,N_records - 1)
        record = list_of_records[n]
        set_names.add(record['NAME'])

    
    #print set_names
        
    # Retrieve all record ids with any of these names
    db_manager = DatabaseManager()    
    str_list_names = "("+",".join(['"%s"' % name for name in set_names]) + ")"
    #print str_list_names
    query ='SELECT id FROM %s WHERE NAME IN %s' % (tablename,str_list_names)
    print query

    # list of the ids of all the records with any of the names in set_names
    list_ids = db_manager.runQuery('SELECT id FROM %s WHERE NAME IN %s' %(tablename,str_list_names))
    list_ids = [x[0] for x in list_ids]
    print len(list_ids)

    
    # list of the identities of all the records
    # retrieved above.
    set_identities = set()

    # A dictionary that for each "main" identity,
    # gives a list consisting of that identity
    # and a few of its "neighbor" identities.
    dict_identities = {}

    counter_missing = 0
    for record_id in list_ids:
        try:
            identity_main = dict_ids_full[record_id] 
            set_identities.add(identity_main)
            if identity_main not in dict_identities:
                dict_identities[identity_main] = range(identity_main - 3, identity_main +3)
                
        except KeyError:
            counter_missing += 1
    print len(set_identities), counter_missing


    # list of ids of all records belonging to any
    # of the identities found above. At this point,
    # we may wish to include records from a few 
    # neighboring identities as well.
    list_ids_expanded = []
    for identity in set_identities:
        list_ids_expanded += dict_identities_full[identity]
    print len(list_ids_expanded)




    list_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION','id']

    # print the records in order of their identities.
    f1 = open('validation.txt','w')
    f2 = open('validation.html','w')
    f2.write("<head><style>"
             + css_code

             + "</style>"
             + "</head>")

    dataframe_data = []
    person_counter = 0
    page_size = 20

    # Each identity_main will get its own page which will
    # consist of identitu_main and all its neighbors
    for identity_main, list_neighboring_identities in dict_identities.iteritems():
        significant = lambda x: '<span class="significant">%s</span>' % x if identity == identity_main else x
        for identity in list_neighboring_identities:
            new_block = []
            for record_id in dict_identities_full[identity]:
                r = dict_of_records[record_id]
                new_row = [r[field] for field in list_fields] 
                new_row = ["" if s is None else s.encode('ascii', 'ignore') if isinstance(s, unicode) else s  for s in new_row ] 
                new_row = [significant(x) for x in new_row]
                new_block.append(new_row)
            dataframe_data += new_block
            dataframe_data += [["" for i in range(len(dataframe_data[0]) - 2)] + ["|"] + [""] for j in range(3)]
                

        # Save a group of blocks to file
        df = pd.DataFrame(dataframe_data, columns=list_fields )
        df.set_index('id', inplace=True)
        f1.write(df.to_string(justify='left').encode('ascii', 'ignore'))
        f2.write(df.to_html(escape=False).encode('ascii', 'ignore'))
        f2.write("<br/><br/>")
        
        # Reset the output buffer
        list_id = []
        dataframe_data = []
    f1.close()
    f2.close()










