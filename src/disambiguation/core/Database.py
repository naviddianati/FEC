'''
Created on Jun 26, 2014

@author: navid
'''

import datetime
import MySQLdb as mdb
from Record import Record
import states

class DatabaseManager:
    '''Generic class for interacting with MySQL server '''

    def __init__(self):
        
        # Establish connection
        self.connection = self.db_connect()

        pass
       

    # establish and return a connection to the MySql database server
    def db_connect(self):
        con = None
        con = mdb.connect(host='localhost',  # hostname
                           user='navid',  # username                
                           passwd='YOURMYSQLPASSWORD',  # passwor
                           db='FEC',  # database 
                           use_unicode=True,
                           charset="utf8"
        )
        if con == None: print("Error connecting to MySql server")
        return con  
    
    def runQuery(self, query):
        if self.connection is None:
            print "No database connection. Aborting"
            return
        cur = self.connection.cursor()
        # cur.execute("select NAME,ZIP_CODE,EMPLOYER,TRANSACTION_DT from newyork order by NAME limit 1000 ;")
        cur.execute(query)
        return  cur.fetchall()

    

    
    
    
    
    
    
    
    
    
    
class FecRetriever(DatabaseManager):
    ''' subclass of DatabaseManager with a method specifically to retrieve our desired data
    from MySQL database.'''
    def __init__(self, table_name, query_fields, limit, list_order_by, where_clause='', require_id=True):
        
        DatabaseManager.__init__(self)

        # process the table_name arg 
        self.table_name = table_name
        self.query_fields = query_fields
        self.list_of_records = []
        self.query = ''
        self.where_clause = where_clause
        self.require_id = require_id
        
        # process the limit arg
        if limit:
            self.limit = " limit %d,%d " % (limit[0], limit[1])
        else:
            self.limit = ""
        
        # process the order_by arg    
        if list_order_by:
            self.order_by = " order by %s " % ",".join(list_order_by)
        else:
            self.order_by = ""
        
#         record_start,record = 1
#         record_no = 5000
        
    def retrieve(self):
        # Get string list from MySQL query and set it as analyst's list_of_records_identifier
        # query_result = runQuery("select " + ','.join(identifier_fields) + " from newyork_addresses where NAME <> '' order by NAME limit " + str(record_start) + "," + str(record_no) + ";")
        query = "select " + ','.join(self.query_fields) + " from " + self.table_name + self.where_clause + self.order_by + self.limit + ";"
        print query
        self.query = query
        
        query_result = self.runQuery(query)
        
        # Convert strings to upper case, dates to date format.
        tmp_list = [[s.upper() if isinstance(s, basestring) else s.strftime("%Y%m%d") if  isinstance(s, datetime.date) else s  for s in record] for record in query_result]
        
        
        self.list_of_records = []
        for counter, item in enumerate(tmp_list):
            r = Record()
            for i, field in enumerate(self.query_fields):
                r[field] = item[i]
            
            # I require that each row have a unique "id" column
            try:
                r.id = r['id']
            except KeyError:
                if self.require_id :
                    raise Exception("ERROR: record does not have 'id' column")
                pass
            
            self.list_of_records.append(r) 
        

    def getRecords(self):
        return self.list_of_records
    
    def getQuery(self):
        return self.query









class IdentityManager(DatabaseManager):
    '''
    DatabaseManager subclass that interacts with the 'identities' MySQL
    table and retrieves cluster membership data.
    '''
    def __init__(self, state, table_name = 'identities', list_order_by = "", where_clause=''):
        DatabaseManager.__init__(self)
        
        self.dict_id_2_identity = {}
        '''@ivar: dictionary mapping each record id to its corresponding identity.'''
        
        self.dict_identity_2_list_ids = {}
        '''@ivar: dictionary mapping each identity to the list of its corresponding record ids.'''
        
        # process the order_by arg    
        self.table_name = table_name

        self.state = state
        
        if state != "USA": 
            state_abbr = states.dict_state_abbr[state]
            # Process the where clause. 
            if where_clause == "":
                where_clause = " WHERE identity like '%%%s%%' " % state_abbr
            else:
                where_clause += " AND identity like '%%%s%%' " % state_abbr
        self.where_clause = where_clause

        
        if list_order_by:
            self.order_by = " order by %s " % ",".join(list_order_by)
        else:
            self.order_by = ""
            
        
        
        
        
        

    def fetch_dict_id_2_identity(self):
        query = "select id,identity from " + self.table_name + self.where_clause + self.order_by  + ";"
        print query
        self.query = query
        query_result = self.runQuery(query)
        # Populate self.dict_id_2_identity
        self.dict_id_2_identity = {int(r_id):identity for r_id,identity in query_result}
        del query_result    
    
    def fetch_dict_identity_2_id(self):
        query = "select id,identity from " + self.table_name + self.where_clause + self.order_by  + ";"
        print query
        self.query = query
        query_result = self.runQuery(query)
        # Populate self.dict_identity_2_list_ids
        for r_id,identity in query_result:
            try:
                self.dict_identity_2_list_ids[identity].append(int(r_id))
            except:
                self.dict_identity_2_list_ids[identity] = [int(r_id)]
                
        
    
    
    







if __name__ == "__main__":
    
    
    fr = DatabaseManager(table_name="california", query_fields=["NAME", "CITY", "CMTE_ID", "TRAN_ID"], limit=(1, 1000000), list_order_by=["NAME"])
    fr.retrieve()
    for record in fr.list_of_records:
        print record


