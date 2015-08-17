'''
This module provides methods for searching the FEC database using 
various types of queries.
'''
from disambiguation.core import Database, Record
import utils


class SearchEngine():
    '''
    Class for searching the FEC database (MySQL table) using various
    queries, including those defined by regex patterns.
    @status: currently we can use queries consisting of regex patterns
    for different record fields. 
    '''
    
    def __init__(self):
        self.db = Database.DatabaseManager()
        list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
        list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
        self.query_fields = list_tokenized_fields + list_auxiliary_fields


    def __get_records_from_query_result(self,result):
        '''
        Process the result returned from a search query
        and convert to a list of records.
        '''
        # Convert strings to upper case, dates to date format.
        tmp_list = [[s.upper() if isinstance(s, basestring) else s.strftime("%Y%m%d") if  isinstance(s, utils.datetime.date) else s  for s in record] for record in result]


        self.reset_buffer()
        for counter, item in enumerate(tmp_list):
            r = Record.Record()
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

    

    def search_regex(self, name="", state="", city="", employer="", occupation=""):
        '''
        Search the FEC database for records with given patterns
        in the specified fields.
        @param name: regex pattern for name.
        '''
        tablename = utils.config.MySQL_table_state_combined % state
        whereclause = []
        whereclause.append("NAME regexp '%s'" % name if name else "")
        whereclause.append("STATE regexp '%s'" % state if state else "")
        whereclause.append("CITY regexp '%s'" % city if city else "")
        whereclause.append("EMPLOYER regexp '%s'" % employer if employer else "")
        whereclause.append("OCCUPATION regexp '%s'" % occupation if occupation else "")
        whereclause = [x for x in whereclause if x]
        whereclause = " AND ".join(whereclause)
        whereclause = "WHERE %s" % whereclause if whereclause else ""
        query = "SELECT %s FROM %s %s;" % (','.join(self.query_fields), tablename, whereclause)
        
        result = self.db.runQuery(query)
        self.__get_records_from_query_result(result)
        return self.list_of_records
    
    
    def get_identities(self, list_of_records=[]):
        '''
        Retrieve the identities of records in list_of_records
        from database.
        @param list_of_records: list of records. If empty, use 
            self.list_of_records.
        '''
        if not list_of_records:
            list_of_records = self.list_of_records
            
        tablename = utils.config.MySQL_table_identities
        list_ids_str = "(%s)" % ",".join([str(r.id) for r in list_of_records])
        query = "SELECT id,identity from %s WHERE id in %s" % (tablename, list_ids_str)
        result = list(self.db.runQuery(query))
        return result
        
    
    
    def print_buffer(self):
        '''
        print the contents of self.list_of_records to stdout.
        '''
        for r in self.list_of_records:
            print r.toString()
            
            
        
    def reset_buffer(self):
        '''
        Clear self.list_of_records
        '''
        self.list_of_records = []
        
        
        
        
if __name__=="__main__":
    sdb = SearchEngine()
    
    for r in sdb.search_regex(name="FLANNIGAN.*", employer="", city=""):
        print r.toString()
