'''
This module defines the L{DatabaseManager} class and its subclasses,
used for interacting with the MySQL server for retrieving records from
the state and national tables, writing to the identities table, retrieving
the computed identities from the C{identities} table, etc.
'''

import datetime
import MySQLdb as mdb

import Record
import states
import utils

class DatabaseManager:
    '''
    Base class for interacting with MySQL server. It implements a connection
    method and a runQuery method.
    '''

    def __init__(self):
        self.connection = self.db_connect()



    def db_connect(self):
        '''
        Extablish and return a connection to the MySQL database server.
        '''
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
        '''
        Run a MySQL query and return the result.
        '''
        if self.connection is None:
            print "No database connection. Aborting"
            return
        cur = self.connection.cursor()
        cur.execute(query)
        return  cur.fetchall()







class FecRetrieverByID(DatabaseManager):
    '''
    Subclass that retrieves records with ids in a given list of ids.
    '''

    def __init__(self, table_name):
        DatabaseManager.__init__(self)

        self.temp_table = ''
        self.table_name = table_name



    def __get_temp_table(self):
        '''
        Return the name of a temp table that can be safely used by
        this instance.
        '''
        self.temp_table = "tmp_" + utils.get_random_string(20)
        query1 = "DROP TABLE IF EXISTS %s;" % self.temp_table
        query2 = "CREATE TABLE %s (id INT  PRIMARY KEY);" % self.temp_table
        self.runQuery(query1)
        self.runQuery(query2)



    def __populate_temp_table(self, list_ids):
        '''
        Insert the ids into the temp table.
        '''
        print "inserting..."
        for rid in list_ids:
            query = "INSERT INTO %s  value (%d);" % (self.temp_table, rid)
            self.runQuery(query)
        print "done."




    def __del_temp_table(self):
        '''
        Delete the allocated temp table from database.
        '''
        query = "DROP TABLE %s;" % self.temp_table
        self.runQuery(query)


    def retrieve(self, list_ids, query_fields):
        '''
        Retrieve the rows with ids in list_ids.
        '''

        self.__get_temp_table()

        t1 = utils.time.time()
        self.__populate_temp_table(list_ids)
        t2 = utils.time.time()
        print "Done in %f seconds" % (t2 - t1)

        # Retrieve the rows from the join
        query = "SELECT " + ','.join(query_fields) + " FROM " + self.table_name + " JOIN " + self.temp_table + " USING (id) ;"
        print query
        t1 = utils.time.time()
        query_result = self.runQuery(query)
        t2 = utils.time.time()
        print "Done in %f seconds" % (t2 - t1)

        # Cleanup
        self.__del_temp_table()


        # Convert strings to upper case, dates to date format.
        tmp_list = [[s.upper() if isinstance(s, basestring) else s.strftime("%Y%m%d") if  isinstance(s, datetime.date) else s  for s in record] for record in query_result]


        self.list_of_records = []
        for counter, item in enumerate(tmp_list):
            r = Record.Record()
            for i, field in enumerate(query_fields):
                r[field] = item[i]

            # I require that each row have a unique "id" column
            try:
                r.id = int(r['id'])
            except KeyError:
                if self.require_id :
                    raise Exception("ERROR: record does not have 'id' column")
                pass

            self.list_of_records.append(r)


    def getRecords(self):
        return self.list_of_records










class FecRetriever(DatabaseManager):
    '''
    subclass of DatabaseManager with a method specifically to
    retrieve our desired data from MySQL database.
    '''
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


    def getRecords(self):
        '''
        Return C{self.list_of_records} which is loaded by C{self.retrieve}
        '''
        return self.list_of_records

    def getQuery(self):
        return self.query









class IdentityManager(DatabaseManager):
    '''
    DatabaseManager subclass that interacts with the 'identities' MySQL
    table and retrieves cluster membership data.
    @ivar dict_id_2_identity: dictionary mapping each record id to its
    corresponding identity.
    @ivar dict_identity_2_list_ids: dictionary mapping each identity to the
    list of its corresponding record ids.
    @ivar dict_identity_adjacency: dictionary mapping a (sorted) tuple of
    identity identifiers to a tuple of numerical scores indicating their relationship.
    The value is a tuple of 3 integers: C{(inconsistent, mayberelated, related)}.
    Each element in this tuple is a weighted count of inter-identity record-pair
    comparison results. C{inconsistent} counts the number of record pairs between
    the two identities with a strong inconsistency. C{mayberelated} counts the number
    of results that aren't conclusive either way but suggest some similarity.
    C{related} counts the number of results that indicate a clear/strong link.
    @cvar table_name_identities: name of the MySQL table containing the identities,
    that is, a unique record id "id" column and an "identity" column.
    @cvar table_name_identity_adjacency: name of the MySQL table containing the
    inferred relationships between identities. This table is defined by the columns:
    C{(identity1 VARCHAR(24), identity2 VARCHAR(24), no INT, maybe INT, yes INT, PRIMARY KEY ("identity1","identity2"))}.
    '''
    
    table_name_identities = 'identities'
    table_name_identity_adjacency = 'identities_adjacency'
    

    def __init__(self, state, list_order_by="", where_clause=''):
        DatabaseManager.__init__(self)

        # dictionary mapping each record id to its corresponding identity.
        self.dict_id_2_identity = {}

        # dictionary mapping each identity to the list of its corresponding record ids.
        self.dict_identity_2_list_ids = {}

        # dictionary mapping a (sorted) tuple of identity identifiers
        # to a numerical score indicating their relationship.
        self.dict_identity_adjacency = {}



        # Query that will create table identities
        self.query_create_table_identities = 'CREATE TABLE %s ( id INT PRIMARY KEY, identity VARCHAR(24));' % IdentityManager.table_name_identities

        # Query that will create table identities_adjacency
        self.query_create_table_identities_adjacency = \
            'CREATE TABLE identities_adjacency (identity1 VARCHAR(24), identity2 VARCHAR(24), no INT, maybe INT, yes INT, PRIMARY KEY (identity1,identity2)  );'

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

        # Initialize the "identities" and "identities_adjacency" tables
        self.__init_table_identities()
        self.__init_table_identities_adjacency()



    def generate_dict_identity_adjacency(self, list_record_pairs, overwrite=False):
        '''
        Compute L{self.dict_identity_adjacency} from a list containing
        results of pairwise record comparisons. This is a stage2 operation.
        It's assumed that stage1 identities exist and most records are
        linked to an identity already. Here, we infer connections between
        those stage1 identities by analyzing the comparison results for
        pairs of records belonging to different identities in stage1.
        @param list_record_pairs: a list where each item is of the form
        C{((rid1,rid2), result)}. Each item is then the result of a record
        pair comparison.
        @requires: a populated "identities" table.
        '''
        counter_has_identity = 0
        counter_result_none = 0
        # tmp dict that will map each identity pair to a list
        # of results from all their inter-cluster record pair
        # comparison results. Each list will be interpreted to
        # give a final relationship code for each identity pair.
        dict_tmp = {}

        if overwrite:
            self.dict_identity_adjacency = {}

        if not self.dict_id_2_identity:
            self.fetch_dict_id_2_identity()

        # Loop through the list of record pairs and their result.
        for r_pair, result in list_record_pairs:
            has_identity1 = True
            has_identity2 = True
            
            # if records are completely unrelated. Don't bother
            # registering their relationship.
            if result is None: 
                counter_result_none += 1
                continue

            rid1, rid2 = r_pair
            try:
                identity1 = self.dict_id_2_identity[rid1]
            except KeyError:
                has_identity1 = False

            try:
                identity2 = self.dict_id_2_identity[rid2]
            except KeyError:
                has_identity2 = False

            # If both have associateed identities, add result
            # to the list of results for that pair of identities.
            if has_identity1 and has_identity2:
                counter_has_identity += 1
                key = tuple(sorted([identity1, identity2]))
                try:
                    dict_tmp[key].append(result)
                except:
                    dict_tmp[key] = [result]
            elif has_identity1:
                # TODO: set rid2's identity equal to identity2
                pass
            elif has_identity2:
                # TODO: set rid1's identity equal to identity1
                pass
            else:
                # TODO: none has an identity, create a new identity
                # for them.
                pass

        # Now, go through dict_tmp and for each identity pair
        # interpret all its results and make a final judgment
        # on the relationship between the identities.
        for key, list_results in dict_tmp.iteritems():
            # Loop through all results for this identity pair
            # and update
            result_no = 0
            result_maybe = 0
            result_yes = 0

            for result in list_results:
                if result < 0:
                    result_no += 1
                elif result == 0:
                    result_maybe += 1
                elif result > 0:
                    result_yes += 1
                    
            self.dict_identity_adjacency[key] = (result_no, result_maybe, result_yes)
            




    def export_identities_adjacency(self):
        '''
        Export the contents of L{self.dict_identity_adjacency} to the 
        table defined by L{IdentityManager.table_name_identity_adjacency}.
        '''
        for key,result in self.dict_identity_adjacency.iteritems():
            #print 'key: ', key
            identity1,identity2 = key
            result_no, result_maybe, result_yes = result
            query = 'INSERT INTO %s (identity1,identity2,no,maybe,yes) VALUES ("%s", "%s", %d, %d, %d);' \
                   % (IdentityManager.table_name_identity_adjacency, identity1,identity2, result_no, result_maybe, result_yes)
            self.runQuery(query)
        self.connection.commit()
        self.connection.close() 
        
        
        
        
        

    def drop_table_identities(self):
        '''
        Drop the table "identities" if exists
        '''
        query = "DROP TABLE IF EXISTS %s;" % IdentityManager.table_name_identities
        self.runQuery(query)



    def drop_table_identities_adjacency(self):
        '''
        Drop the table "identities_adjacency" if exists
        '''
        query = "DROP TABLE IF EXISTS %s;" % IdentityManager.table_name_identity_adjacency
        self.runQuery(query)



    def __init_table_identities(self):
        '''
        Check whether the "identities" table exists and
        create it if not.
        '''
        query = "SELECT COUNT(*) FROM information_schema.tables \
                    WHERE table_schema = 'FEC' \
                    AND table_name = '%s';" % IdentityManager.table_name_identities
        result = self.runQuery(query)
        if result[0][0] == 0:
            print "Table '%s' doesn't exist. Creating it." % IdentityManager.table_name_identities
            self.runQuery(self.query_create_table_identities)
        else:
            print "Table '%s' exists." % IdentityManager.table_name_identities


    def __init_table_identities_adjacency(self):
        '''
        Check whether the "identities_adjacency" table exists
        and create it if not.
        '''
        query = "SELECT COUNT(*) FROM information_schema.tables \
                    WHERE table_schema = 'FEC' \
                    AND table_name = 'identities_adjacency';"

        result = self.runQuery(query)
        if result[0][0] == 0:
            print "Table 'identities_adjacency' doesn't exist. Creating it."
            self.runQuery(self.query_create_table_identities_adjacency)
        else:
            print "Table 'identities_adjacency' exists."
            self.__truncate_table_identities_adjacency()

    def __truncate_table_identities_adjacency(self):
        query = "TRUNCATE TABLE %s;" % IdentityManager.table_name_identity_adjacency
        self.runQuery(query)


    def fetch_dict_id_2_identity(self):
        '''
        Fetch from the "identities" table the requested set of identities and
        record ids, and populate the dict self.dict_id_2_identity which maps
        every record id to its corresponding identity.
        '''
        query = "select id,identity from " + IdentityManager.table_name_identities + self.where_clause + self.order_by + ";"
        print query
        self.query = query
        query_result = self.runQuery(query)
        # Populate self.dict_id_2_identity
        self.dict_id_2_identity = {int(r_id):identity for r_id, identity in query_result}
        del query_result

    def fetch_dict_identity_2_id(self):
        '''
        Fetch from the "identities" table the requested set of identities and
        record ids, and populate the dict self.dict_identity_2_list_ids which maps
        every identity to a list of record ids associated with it.
        '''
        query = "select id,identity from " + IdentityManager.table_name_identities + self.where_clause + self.order_by + ";"
        print query
        self.query = query
        query_result = self.runQuery(query)
        # Populate self.dict_identity_2_list_ids
        for r_id, identity in query_result:
            try:
                self.dict_identity_2_list_ids[identity].append(int(r_id))
            except:
                self.dict_identity_2_list_ids[identity] = [int(r_id)]

        del query_result










if __name__ == "__main__":


    fr = DatabaseManager(table_name="california", query_fields=["NAME", "CITY", "CMTE_ID", "TRAN_ID"], limit=(1, 1000000), list_order_by=["NAME"])
    fr.retrieve()
    for record in fr.list_of_records:
        print record


