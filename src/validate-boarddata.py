from disambiguation.core import Database
from disambiguation.core import Project
from disambiguation.core import Tokenizer 
from disambiguation.core import Record 
from disambiguation.core import utils 


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

        
def get_auxilliary_data(list_data):
    '''
    Return html code for the auxilliary data div.
    @param list_data: list of the data items. Each element must be a tuple 
    of strings C{(title:content)}.
    '''
    html = ''
    for title,content in list_data:
        html += "<div class = 'aux-data-item-title'> %s </div>\n" % title
        html += "<div class = 'aux-data-item-content'> %s </div>\n" % content
    return html





def format_date(s):
    if s:
        return "%s-%s-%s" % (s[:4],s[4:6],s[6:8])
    else:
        return ''





def worker_get_similar_records_db(target_record):
    '''
    Worker function that fetches all records similar to the
    specified record from database, writes them to file and
    returns them.
    @param target_record: the record for which similar records
        are to be fetched from the database. This record can be
        an artificial record created from a query.
    '''
    
    db = Database.DatabaseManager()

    list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    all_fields = list_tokenized_fields + list_auxiliary_fields

    firstname = target_record['N_first_name']
    lastname = target_record['N_last_name']
    middlename = target_record['N_middle_name']

    db = Database.FecRetriever(table_name='individual_contributions',
    #db = Database.FecRetriever(table_name='newyork_combined',
                      query_fields=all_fields,
                      limit='',
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      where_clause=" WHERE NAME REGEXP '[[:<:]]%s.*[[:<:]]%s'" %(lastname,firstname) 
    )
    try:
        db.retrieve()
    except Exception, e:
        with open('error-log.txt','a') as f:
            f.write("Unable to retrieve similar records for this record:\n")
            f.write(target_record['NAME'] + "\n")
            f.write(str(e) + "\n")
            f.write("="*72 + "\n")
    print "records retrieved"
    list_records = db.getRecords() 
 
    print "saving to file"
    with open(firstname + "-" + lastname + ".txt", 'w') as f:
        f.write("="*70 + "\n")
        f.write("%s %s %s\n" %(target_record['N_first_name'], target_record['N_middle_name'], target_record['N_last_name']))
        f.write("="*70 + "\n")
        for record in list_records:
            f.write('  '.join([str(record[field]) for field in all_fields]) + "\n")
    return target_record, list_records
            


    


def get_list_target_records():
    '''
    Return a list of target records. For each record in this
    list, we will generate a handcoding page as defined by 
    generate_coding_page(). 
    @return: list of target records.
    '''
    state = 'boarddata'

    # read rows from the board data file.    
    data = pd.read_csv('/nfs/home/navid/data/FEC/zubin/sample_data.csv')
    
    list_of_records = []
    counter = 0
    for x in data.iterrows():
        try:
            r = Record.Record()
            r.id = x[1]['directorid']
            r['NAME'] = x[1]["Director Name"].encode('ascii', 'ignore')
            r['EMPLOYER'] = x[1]["employment_Company Name"].encode('ascii', 'ignore')
            r['OCCUPATION'] =  x[1]["employment_Role"].encode('ascii', 'ignore')
            list_of_records.append(r)
            counter += 1
        except Exception, e:
            print e
        #if counter > 15: break
    # Normalize and tokenize the target records
    project = Project.Project(1)
    project.putData('state', state)
    project.putData('list_tokenized_fields',['NAME','EMPLOYER','OCCUPATION'])
    tokenizer = Tokenizer.Tokenizer()
    project.tokenizer = tokenizer
    tokenizer.project = project
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(project['list_tokenized_fields'])
    
    tokenizer.tokenize()
    list_of_records = tokenizer.getRecords()
    return list_of_records




def get_identity_data(state='USA'):
    '''
    Load identity data from database.
    @return: dict_id_2_identity, dict_identity_2_list_ids
    '''
    idm = Database.IdentityManager(state)
    idm.fetch_dict_id_2_identity()
    idm.fetch_dict_identity_2_id()
    # Maps id to identity
    dict_id_2_identity = idm.dict_id_2_identity
    dict_identity_2_list_ids = idm.dict_identity_2_list_ids
    return dict_id_2_identity, dict_identity_2_list_ids
    
    
    
def get_dict_aux_data(record):
    '''
    return a list of the auxilliary data to be
    displayed on the coding page for the given
    target record.
    '''
    list_data = [("Name", record['NAME']),
                 ("Employer", record['EMPLOYER']),
                 ("Occupation", record['OCCUPATION'])]
    return list_data


def get_coding_page(target_record, dict_identities):
    '''
    Generate an html page with the tables containing all
    data necessary for coding related to the target record.
    @param target_record: Record instance. Can be an artificial record
        created from a query.
    @param dict_identities: dict {identity:list_of_records} for all identities
        containing records similar to target_record.
    @return: html code for the coding page.
    '''
    list_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION','id']
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
    table = html_table()
    table.add_header([dict_fields[x] for x in list_fields] + ['identity'] , row_classes = ['table-header'])

    # loop through all identities that must be displayed on page.
    print "Len(dict_identities): ", len(dict_identities)
    for  identity, list_current_records in dict_identities.iteritems():
        new_block = []
        for r in list_current_records:
            r['TRANSACTION_DT'] = format_date(r['TRANSACTION_DT'])
            new_row = [r[field] for field in list_fields] + [identity] 
            new_row = ["" if s is None else s.encode('ascii', 'ignore') if isinstance(s, unicode) else s  for s in new_row ] 
            row_classes = ['record', 'noselect', 'identity-'+identity]
            row_classes.append('score-no') 
            
            column_classes = ['' for i in range(len(new_row))]
            column_classes[list_fields.index('id')] = "id"
            column_classes[-1] = "identity" 
            table.add_row(new_row, row_classes = row_classes, row_id=str(r['id']), column_classes = column_classes)

        table.add_row([], row_classes=["separator"])
    return table.to_html()


    
    
    
def generate_coding_page_multiproc(list_of_records, num_procs, dict_id_2_identity, dict_identity_2_list_ids):
    '''
    Using a pool of processes, generate coding pages for the target
    records in list_of_records. To do this, first fetch a list of
    similar records for each target record.
    @param list_of_records: list of target records.
    '''
    html = ''
    
    # For each target record, get a list of similar records.
    pool = utils.multiprocessing.Pool(num_procs)


    # For each target record, analyze its list of
    # similar records.
    page_counter = 0
    for target_record,list_similar_records in pool.imap(worker_get_similar_records_db, list_of_records):
        print "Received results..."
        print len(list_similar_records)
            
        dict_similar_records = {r.id:r for r in list_similar_records}
        
        # Get the identities of all similar records
        set_identities = set()
        counter_no_identity = 0
        for similar_record in list_similar_records:
            try:
                set_identities.add(dict_id_2_identity[similar_record.id])
            except Exception, e:
                print e
                counter_no_identity += 1
        print "Number of ids without identity: ", counter_no_identity
        print set_identities
    

        for identity in set_identities:
            if identity in dict_identity_2_list_ids:
                print "yes ", identity
            else:
                print "no  ", identity
        

        try:
            # Dict {identity:list of records} for all identities 
            # containing records similar to target record.        
            dict_identities = {identity: [dict_similar_records[r_id] for r_id in dict_identity_2_list_ids[identity]]\
                     for identity in set_identities   }
            
            # Html code for the page for target_record
            html = get_coding_page(target_record, dict_identities)
        
            # write main content of coding page to file
            with open('%d.html' % (page_counter + 1), 'w') as f:
                f.write(html)
                
            with open("%d-aux.html" % (page_counter + 1), 'w') as f:
                f.write(get_auxilliary_data(get_dict_aux_data(target_record)))
          
            page_counter += 1
        except Exception, e:
            print e

    


if __name__ == "__main__":

    # Set of "records" we want to find matches for.
    # A records can be an artificial records build
    # from a query.
    list_target_records = get_list_target_records()

    # load identity data.
    dict_id_2_identity, dict_identity_2_list_ids = get_identity_data()
    #dict_id_2_identity, dict_identity_2_list_ids = {},{}
    
    # Generate the pages
    generate_coding_page_multiproc(list_target_records, 22, dict_id_2_identity, dict_identity_2_list_ids)
    

    














