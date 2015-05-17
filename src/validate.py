from disambiguation.core import Database


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


class html_table():
    def __init__(self):
        self.has_header = False
        self.html = "<table>"

    def add_header(self, list_columns, row_id = None, row_classes = [] ):
        if self.has_header:
            print "Table already has header"
            return
        self.add_row(list_columns, row_id, row_classes, is_header = True)
        self.has_header = True



    def add_row(self, list_columns, row_id = None, row_classes = [], is_header = False, column_classes = None):
        """
        Add a single row to the table. Specify a css id or a list of classes for the row.
        column_classes is either None, or else, it's a list of the same length as list_columns
        """
        if column_classes and (len(column_classes) != len(list_columns)):
            raise ValueError("Error: column_classes must have same length as list_columns.")

        if column_classes is None:
            column_classes = ["" for i in range(len(list_columns))]

        row_tag = "th" if is_header else "td"
        row = "<tr %s %s>\n" % ( "" if not row_id else "id='%s'" % row_id  , "" if not row_classes else "class=" + "'" + ' '.join(row_classes) + "'" )

        row += "".join(["    <%s class='%s'> %s </%s>\n" % (row_tag, column_classes[i], column_value, row_tag) for i, column_value in enumerate(list_columns)])
        row += "</tr>\n" 
        self.html += row

    def to_html(self):
        self.html += "</table>\n"
        return self.html

        
def get_auxilliary_data(dict_data):
    '''
    Return html code for the auxilliary data div.
    @param dict_data: dictionary of the data items.
    '''
    html = ''
    for title,content in dict_data.iteritems():
        html += "<div class = 'aux-data-item-title'> %s </div>\n" % title
        html += "<div class = 'aux-data-item-content'> %s </div>\n" % content
    return html



def get_identities_from_db(state_abbr='DE'):
    '''Export the calculated identities of the records to a database table called "identities".'''
    db_manager = Database.DatabaseManager()    

    
    result = db_manager.runQuery('SELECT id,identity FROM identities WHERE identity like "%%%s%%"' % state_abbr)
    db_manager.connection.commit()
    db_manager.connection.close()
    return result





def load_records(param_state = "delaware", all_fields = [], whereclause = ' ', record_limit = (0,5000000), do_sort = True):

    record_start = record_limit[0]
    record_no = record_limit[1]
    table_name = param_state + "_combined"

    if not all_fields:
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
    
    retriever = Database.FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by= ['NAME', "TRANSACTION_DT", "ZIP_CODE"] if do_sort else [],
                      where_clause=whereclause
                      )
    retriever.retrieve()
    
    list_of_records = retriever.getRecords()
    return list_of_records


def format_date(s):
    if s:
        return "%s-%s-%s" % (s[:4],s[4:6],s[6:8])
    else:
        return ''


if __name__ == "__main__":

    N_names =10  
    N_records = 60000
    tablename = "delaware_combined"

    list_ids_full =  get_identities_from_db()
    
    # Maps id to identity
    dict_id_2_identity = {x[0]: x[1] for x in list_ids_full}
    # print "dict_id_2_identity: ", dict_id_2_identity

    # dictionary mapping identity to list of associated ids
    dict_identity_2_list_ids = {}
    for record_id, identity in dict_id_2_identity.iteritems():
        try:
            dict_identity_2_list_ids[identity].append(record_id)
        except KeyError:
            dict_identity_2_list_ids[identity] = [record_id]


    # list and dict of all loaded records
    list_of_records_names_only = load_records(record_limit=(0,N_records), all_fields = ['id','NAME'], do_sort = False)
    N_records = len(list_of_records_names_only)
    dict_of_records_names_only = {r['id']:r for r in list_of_records_names_only}
    print "Names loaded..."

    # Select N_names random names from the retrieved
    # records.
    set_names = set()
    while len(set_names) < N_names:
        n = random.randint(0,N_records - 1)
        record = list_of_records_names_only[n]
        set_names.add(record['NAME'])

    
    # Kill the list. No longer needed
    list_of_records_names_only = None
        
    # Retrieve all record ids with any of these names
    db_manager = Database.DatabaseManager()    
    str_list_names = "("+",".join(['"%s"' % name for name in set_names]) + ")"
    #print str_list_names
    query ='SELECT id FROM %s WHERE NAME IN %s' % (tablename,str_list_names)
    print query

    # list of the ids of all the records with any of the names in set_names
    #list_ids = db_manager.runQuery('SELECT id FROM %s WHERE NAME IN %s' %(tablename,str_list_names))
    #list_ids = [x[0] for x in list_ids]
    #print len(list_ids)

    
    # list of the identities of all the records
    # retrieved above.
    set_identities = set()

    # A dictionary that for each "main" identity,
    # gives a list consisting of that identity
    # and a few of its "neighbor" identities.
    dict_identities = {}

    counter_missing = 0


    # A list of all the records ids we will ever need
    list_all_ids = []   
 
    # for each unique name sampled, find the list of all records with that name,
    # then find the identities of all those records, then pick one of those as
    # the main identity, and display it together with all others and a few 
    # neighbors of each.
    for counter_name, name in enumerate(set_names):
        print "%d --- %s" % (counter_name, name)
        try:
            # list of the ids of all the records with the given name.
            list_ids = db_manager.runQuery('SELECT id FROM %s WHERE NAME ="%s"' %(tablename,name))
            list_ids = [x[0] for x in list_ids]
            list_all_ids += list_ids
        except:
            raise()
        # choose the identity of one of the found records as the main id
        record_id = list_ids[random.randint(0,len(list_ids)-1)]

        try:
            identity_main = dict_id_2_identity[record_id] 
            set_identities.add(identity_main)
            if identity_main not in dict_identities:
                
                # set of identities of all records with the given name
                set_identities_with_given_name = set([dict_id_2_identity[r_id] for r_id in list_ids])

                # list of identities of all records with the given name as well as a few neighbors of each.
                # relevant_identities = list(set([neighbor_identity for center_identity in set_identities_with_given_name\
                #                                             for neighbor_identity in range(center_identity - 3, center_identity +3)]))
                # dict_identities[identity_main] = relevant_identities
                dict_identities[identity_main] = [identity_main]
                # set_identities.update(relevant_identities)
                
        except KeyError:
            counter_missing += 1
    print 'len(set_identities), coutner_missing: ', len(set_identities), counter_missing


    # list of ids of all records belonging to any
    # of the identities found above. At this point,
    # we may wish to include records from a few 
    # neighboring identities as well.
    list_ids_expanded = []
    for identity in set_identities:
        list_ids_expanded += dict_identity_2_list_ids[identity]
    print len(list_ids_expanded)




    list_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION','id']
    dict_fields={"TRANSACTION_DT": "Date",
                 "ZIP_CODE": "zipcode",
                 "CONTRIBUTOR_STREET_1": "Address",
                 "NAME": "Name",
                 "CITY":"City",
                 "STATE":"State",
                 "EMPLOYER":"Employer",
                 "OCCUPATION":"Occupation",
                 "id":"id",
                 "identity":"identity"}

    # print the records in order of their identities.
    dataframe_data = []
    person_counter = 0
    page_size = 20

    print "dict_identities: ", dict_identities


    # Now load only the records we need

    str_list_ids = "("+",".join(['"%s"' % r_id for r_id in list_ids_expanded]) + ")"
    list_of_records = load_records(record_limit=(0,N_records), whereclause=' WHERE id in %s ' % str_list_ids)
    dict_of_records= {r['id']:r for r in list_of_records}

    # all a boolean property to all records. This 
    # property will be a marker of which records 
    # truly belong to the main cluster on each page.
    for record in list_of_records:
        record.ismain = False        


    # Each identity_main will get its own page which will
    # consist of identitu_main and all its neighbors
    page_counter = 0
    for identity_main, list_neighboring_identities in dict_identities.iteritems():
        page_counter += 1
        print page_counter
        #f1 = open('%d.txt' % page_counter,'w')
        f2 = open('%d.html' % page_counter,'w')
        table = html_table()
        table.add_header([dict_fields[x] for x in list_fields] + ['identity'] , row_classes = ['table-header'])

        # list of all record id's on current page
        list_record_ids = [ r_id for identity in list_neighboring_identities for  r_id in dict_identity_2_list_ids[identity] ]

        # which type of error should we deliberately insert?
        # 1 means a non-focus record is inserted into the focus cluster
        # 2 means a focus record is put in a non-focus cluster.
        # Other values mean no error introduced.
        # error_type = random.choice([1,2,3,4])
        error_type = 4


        if error_type in [1,2]:        
            id_random_nonfocus = random.choice(list(set(list_record_ids).difference(set(dict_identity_2_list_ids[identity_main])))) 
            id_random_focus = random.choice(dict_identity_2_list_ids[identity_main])
        else:
            id_random_nonfocus = None
            id_random_focus = None
        

        # if identity_main already has only one record, then either
        # type of error will lead to an ambiguous situation.
        if len(dict_identity_2_list_ids[identity_main]) < 3: error_type = None
        
        # Mark all records belonging to the focus cluster
        for r_id in dict_identity_2_list_ids[identity_main]:
            record = dict_of_records[r_id]
            record.ismain = True
        
        # Introduce the error
        identity_target = None
        # If error type == 1, insert id_random_nonfocus into identity_main
        if error_type == 1:
            pass   
            
        if error_type == 2:
            # The identity into which the record is to be inserted
            identity_target = random.choice(list(set(list_neighboring_identities) - set([identity_main])))

        for identity in list_neighboring_identities:
            # List of ids in current identity cluster
            list_current_ids = sorted(dict_identity_2_list_ids[identity], key = lambda r_id: dict_of_records[r_id]['TRANSACTION_DT'])  
            if error_type == 2:
                # Insert the random focus record into this identity if it's the target identity
                if identity == identity_target: list_current_ids.insert(random.randint(0,len(list_current_ids)-1), id_random_focus)
            if error_type == 1:
                # Inser the random non-focus record into the main identity
                if identity == identity_main: list_current_ids.insert(random.randint(0,len(list_current_ids)-1), id_random_nonfocus)
            
        
            new_block = []
            for record_id in list_current_ids:
                if (error_type == 1) and (identity != identity_main) and (record_id == id_random_nonfocus): continue
                if (error_type == 2) and (identity == identity_main) and (record_id == id_random_focus): continue

                r = dict_of_records[record_id]
                r['TRANSACTION_DT'] = format_date(r['TRANSACTION_DT'])
                new_row = [r[field] for field in list_fields] + [identity] 
                new_row = ["" if s is None else s.encode('ascii', 'ignore') if isinstance(s, unicode) else s  for s in new_row ] 
                #new_block.append(new_row)
                row_classes = ['record', 'noselect']
                if r.ismain: 
                    row_classes.append('ismain')
                    #row_classes = ["focus_identity","record", "score-yes", "noselect"]
                else:
                    pass

                if identity == identity_main:
                    row_classes.append('score-yes')
                else:
                    row_classes.append('score-no') 
                
                column_classes = ['' for i in range(len(new_row))]
                column_classes[list_fields.index('id')] = "id"
                column_classes[-1] = "identity" 
                table.add_row(new_row, row_classes = row_classes, row_id=str(record_id), column_classes = column_classes)
            
            separator_solumn_classes = ['emp' for i in range(len(new_row))] 
             
            table.add_row([], row_classes=["separator"])

        # ummark all records belonging to the focus cluster
        for r_id in dict_identity_2_list_ids[identity_main]:
            record = dict_of_records[r_id]
            record.ismain = False

        f2.write(table.to_html())
        f2.close() 
        list_id = []


        
        
















