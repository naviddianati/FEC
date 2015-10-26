'''
This module defines the L{DatabaseManager} class and its subclasses,
used for interacting with the MySQL server for retrieving records from
the state and national tables, writing to the identities table, retrieving
the computed identities from the C{identities} table, etc.
'''

import datetime
import MySQLdb as mdb

import Record
import Person
import states
import utils
import pandas as pd



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
                           user=utils.config.MySQL_username,  # username
                           passwd=utils.config.MySQL_password,  # passwor
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
    Subclass that retrieves records with ids in a given list of ids. For
    efficiency when a large number of record ids are requested, this task
    is accomplished via a join operation with a temporary table.
    '''

    def __init__(self, table_name):
        DatabaseManager.__init__(self)
        self.dict_of_records = {}
        self.temp_table = ''
        self.table_name = table_name
        self.require_id = False

        list_tokenized_fields = ['NAME', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
        list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
        self.all_fields = list_tokenized_fields + list_auxiliary_fields

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
        @param list_ids: list of record ids tobe retrieved.
        '''
        self.connection.autocommit(False)
        print "inserting..."
        for rid in list_ids:
            query = "INSERT INTO %s  value (%d);" % (self.temp_table, rid)
            self.runQuery(query)
        self.connection.commit()
        self.connection.autocommit(True)
        print "done."
    
        




    def __del_temp_table(self):
        '''
        Delete the allocated temp table from database.
        '''
        query = "DROP TABLE %s;" % self.temp_table
        self.runQuery(query)


    def retrieve(self, list_ids, query_fields=[]):
        '''
        Retrieve the rows with ids in list_ids.
        '''


        self.__get_temp_table()

        t1 = utils.time.time()
        self.__populate_temp_table(list_ids)
        t2 = utils.time.time()
        print "Done in %f seconds" % (t2 - t1)

        if query_fields == []:
            query_fields = self.all_fields
        self.all_fields = query_fields

        fields = ','.join(self.all_fields)
        # Retrieve the rows from the join
        query = "SELECT " + fields + " FROM " + self.table_name + " JOIN " + self.temp_table + " USING (id) ;"
        print query
        t1 = utils.time.time()
        query_result = self.runQuery(query)
        self.query_result = query_result
        t2 = utils.time.time()
        print "Done in %f seconds" % (t2 - t1)

        # Cleanup
        self.__del_temp_table()


        # Convert strings to upper case, dates to date format.
        tmp_list = [[s.upper() if isinstance(s, basestring) else s.strftime("%Y%m%d") if  isinstance(s, datetime.date) else s  for s in record] for record in query_result]
        self.list_results = tmp_list

        self.list_of_records = []
        for counter, item in enumerate(tmp_list):
            r = Record.Record()
            for i, field in enumerate(self.all_fields):
                try:
                    r[field] = item[i]
                except:
                    pass
            # I require that each row have a unique "id" column
            try:
                r.id = int(r['id'])
            except KeyError:
                if self.require_id :
                    raise Exception("ERROR: record does not have 'id' column")
                pass
            except Exception as e:
                print "ERROR", e
                raise
            
            try:
                r['OCCUPATION'] = r['OCCUPATION'].encode('ascii','ignore')
            except:
                pass
            
            try:
                r['EMPLOYER'] = r['EMPLOYER'].encode('ascii','ignore')
            except:
                pass

            # WARNING: switching from list_of_records to dict_of_records
            # self.list_of_records.append(r)
            self.dict_of_records[r.id] = r

    def getRecords(self):
            # WARNING: switching from list_of_records to dict_of_records
        # return self.list_of_records
        return self.dict_of_records.values()


    def set_idm(self, idm):
        '''
        Set self's IdentityManager instance.
        ''' 
        self.idm = idm

    def get_idm(self):
        '''
        Instantiate a new IdentityManager.
        '''
        if self.idm is None:
            self.idm = IdentityManager('USA')
            self.idm.fetch_dict_id_2_identity()
            


    def export_csv(self, filelabel = 'records_export'):
        '''
        @param filelabel: string label for output file. This
        label will be inserted into the csv_exported_state_template
        template.
        '''
        filename = utils.config.csv_exported_state_template  % filelabel
        self.get_idm()
        list_results_updated = []
        # add identities to records
        index_id = self.all_fields.index('id')
        for line in self.list_results:
            line = list(line)
            r_id = line[index_id]
            identity = self.idm.get_identity(r_id)
            line.insert(0,identity if identity else '')
            list_results_updated.append(line)
        header =  ['identity'] +  self.all_fields 
        df = pd.DataFrame(list_results_updated, columns = header)
        df.to_csv(filename, sep = '|', header = header, index = False)
        
        
        
        









class FecRetriever(DatabaseManager):
    '''
    subclass of DatabaseManager with a method specifically to
    retrieve our desired data from MySQL database.
    '''
    def __init__(self, table_name, query_fields=[], limit="", list_order_by="", where_clause='', require_id=True):

        DatabaseManager.__init__(self)

        list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
        list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
        self.all_fields = list_tokenized_fields + list_auxiliary_fields

        # process the table_name arg
        self.table_name = table_name
        self.query_fields = query_fields if query_fields else self.all_fields
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


    def retrieve(self, query = ''):
        # Get string list from MySQL query and set it as analyst's list_of_records_identifier
        # query_result = runQuery("select " + ','.join(identifier_fields) + " from newyork_addresses where NAME <> '' order by NAME limit " + str(record_start) + "," + str(record_no) + ";")
        if not query:
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



class FecExporter(FecRetriever):
    '''
    Subclass of FecRetriever for retrieving records and identities
    and exporting them into more accessible text file formats such
    as CSV.
    @ivar idm: an IdentityManager instance. It can be set after 
    initialization, or if it isn't set at export time, a new instance
    will be instantiated.
    '''
    def __init__(self, state, query_fields=[], limit="", list_order_by="", where_clause='', require_id=True):
        params = locals()
        del params['self']
        del params['state']
        FecRetriever.__init__(self, **params)
        self.table_name = utils.config.MySQL_table_state_combined % state 
        
        list_tokenized_fields = ['NAME', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
        list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
        self.all_fields = list_tokenized_fields + list_auxiliary_fields
        self.query_fields = self.all_fields

        #state
        self.state = state

        # IdentityManager instance
        self.idm = None
    
    def retrieve(self):
        # Get string list from MySQL query and set it as analyst's list_of_records_identifier
        # query_result = runQuery("select " + ','.join(identifier_fields) + " from newyork_addresses where NAME <> '' order by NAME limit " + str(record_start) + "," + str(record_no) + ";")
        query = "select " + ','.join(self.query_fields) + " from " + self.table_name + self.where_clause + self.order_by + self.limit + ";"
        print query
        self.query = query
        query_result = self.runQuery(query)

        # Convert strings to upper case, dates to date format.
        self.list_results = [[s.upper() if isinstance(s, basestring) else s.strftime("%Y%m%d") if  isinstance(s, datetime.date) else s  for s in record] for record in query_result]
    
    def set_idm(self, idm):
        '''
        Set self's IdentityManager instance.
        ''' 
        self.idm = idm

    def get_idm(self):
        '''
        Instantiate a new IdentityManager.
        '''
        if self.idm is None:
            self.idm = IdentityManager('USA')
            self.idm.fetch_dict_id_2_identity()
            


    def export_csv(self, filename = None):
        if filename is None:
            filename = utils.config.csv_exported_state_template  % self.state 
        self.get_idm()
        list_results_updated = []
        # add identities to records
        index_id = self.query_fields.index('id')
        for line in self.list_results:
            line = list(line)
            r_id = line[index_id]
            identity = self.idm.get_identity(r_id)
            line.insert(0,identity if identity else '')
            list_results_updated.append(line)
        header =  ['identity'] +  self.query_fields 
        df = pd.DataFrame(list_results_updated, columns = header)
        df.to_csv(filename, sep = '|', header = header, index = False)
        
        
        
        
        





class IdentityManager(DatabaseManager):
    '''
    DatabaseManager subclass that interacts with the 'identities' and
    'identities_adjacency' MySQL tables and retrieves cluster membership data.
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
    @ivar dict_identity_2_identities: dict mapping a each identity to others it's related to.
    Each value is a dict of C{other_identity: relationship} key-value pairs. More convenient
    to use in place of L{self.dict_identity_adjacency} when we  have a single identity
    and need to find all related identities.
    @ivar dict_persons: dict mapping an C{identity} to a L{Person} instance.

    @cvar table_name_identities: name of the MySQL table containing the identities,
    that is, a unique record id "id" column and an "identity" column.
    @cvar table_name_identity_adjacency: name of the MySQL table containing the
    inferred relationships between identities. This table is defined by the columns:
    C{(identity1 VARCHAR(24), identity2 VARCHAR(24), no INT, maybe INT, yes INT, PRIMARY KEY ("identity1","identity2"))}.
    '''

    import utils
    table_name_identities = utils.config.MySQL_table_identities
    table_name_identity_adjacency = utils.config.MySQL_table_identities_adjacency


    def __init__(self, state, list_order_by="", where_clause=''):
        DatabaseManager.__init__(self)

        # dictionary mapping each record id to its corresponding identity.
        self.dict_id_2_identity = {}

        # dictionary mapping each identity to the list of its corresponding record ids.
        self.dict_identity_2_list_ids = {}

        # dictionary mapping a (sorted) tuple of identity identifiers
        # to a numerical score indicating their relationship.
        self.dict_identity_adjacency = {}

        # dict mapping a each identity to others it's related to
        # Each value is a dict of  other_identity:relationship key-value pairs.
        # Easier to use in place of self.dict_identity_adjacency when
        # we have a single identity and need to find all related identities.
        self.dict_identity_2_identities = {}

        # Query that will create table identities
        self.query_create_table_identities = 'CREATE TABLE %s ( id INT PRIMARY KEY, identity VARCHAR(24));' % IdentityManager.table_name_identities

        # Query that will create table identities_adjacency
        self.query_create_table_identities_adjacency = \
            'CREATE TABLE %s (identity1 VARCHAR(24), identity2 VARCHAR(24), verdict INT, PRIMARY KEY (identity1,identity2)  );' % IdentityManager.table_name_identity_adjacency

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

        
        # Initialize the identities and identities_adjacency tables.
        # Only make sure they exist. DO NOT overwrite them.
        self.init_tables(overwrite = False)
        
        # dict {identity: Person instance}
        self.dict_persons = None


    def init_tables(self, overwrite = False):
        '''
        Initialize the identities and identities_adjacency tables.
        @param overwrite: whether to overwrite the tables if they
        exist already.
        '''
        # Initialize the "identities" and tables
        self.__init_table_identities(overwrite)
        self.__init_table_identities_adjacency(overwrite)


    def get_related_identities(self, identity):
        '''
        Return a list of all identities related to the given identy.
        These include any identity some of whose records were compared to
        the records of the given identity regardless of the (no, maybe, yes)
        values. The decision of which identities should be considered
        linked to the given identity must be made by another function
        using the output of this function.
        @param identity: a string. The stage1 identity of a cluster.
        @return: a dict of C{other_identity: relation} key-value pairs.
        '''
        if not self.dict_identity_2_identities:
            self.load_dict_identity_2_identities()
        try:
            result = self.dict_identity_2_identities[identity]
        except KeyError:
            result = None
        return result


    def get_linked_identities(self, identity, fcn_linked = None):
        '''
        Decide which identities must be considered linked to the given identity,
        that is, "cut the dendrogram". This will be done using the function
        L{self.get_related_identities}, but not all "related" identities will
        necessarily make it to the list of linked identities.
        @param identity: the identity in question.
        @param fcn_linked: a function that decides if two identities that are
        linked by an edge in self.dict_identity_2_identities are close enough
        that they should be considered linked. This function will be used to 
        find all identities that are connected to "identity" through a path
        of such edges. If this function is not provided, a default will be used.
        @return: a list of all identities that we judge to be linked to the
        given identity.
        '''
        def fcn_linked_default(verdict):
            '''
            The default function that decides if two identities that are
            linked by an edge in self.dict_identity_2_identities are close enough
            that they should be considered linked.
            @param verdict: the integer score defining the edge
            between the two identities.
            '''
            x = verdict
            return (x >= 1) 
        

        if fcn_linked is None:
            fcn_linked = fcn_linked_default
            
        def get_neighborhood(identity, set_visited):
            '''
            recursively enumerate all identities in the same connected component as 
            identity according to the the graph of self.get_related_identities. 
            '''
            X = self.get_related_identities(identity)
            if not X: return 
            for neighbor,score in X.items():        
                if neighbor not in set_visited and fcn_linked(score):
                    set_visited.add(neighbor)
                    get_neighborhood(neighbor,set_visited)

        set_visited = set([identity])    
        get_neighborhood(identity,set_visited)
        return list(set_visited)



    def fetch_dict_identity_adjacency(self):
        '''
        Load the L{table_name_identity_adjacency} table into
        L{self.dict_identity_adjacency}.
        '''
        query = "SELECT identity1,identity2, verdict from " + IdentityManager.table_name_identity_adjacency + ";"
        print query
        query_result = self.runQuery(query)
        # Populate self.dict_identity_adjacency
        self.dict_identity_adjacency = {(identity1, identity2) : verdict for identity1, identity2, verdict in query_result}
        del query_result

    def get_ids(self, identity):
        '''
        For the specified identity, return a list of all
        record ids contained in that identity, from
        L{self.dict_identity_2_ids}
        '''
        if not self.dict_identity_2_list_ids:
            print "dict_identity_2_list_ids not loaded. Loading now..."
            self.fetch_dict_identity_2_id()
        try:
            return self.dict_identity_2_list_ids[identity]
        except:
            return []



    def get_identity(self, rid):
        '''
        For the specified record id, retrieve the identity
        from L{self.dict_id_2_identity}.
        '''
        if not self.dict_id_2_identity:
            print "dict_id_2_identity not loaded. Loading now..."
            self.fetch_dict_id_2_identity()

        try:
            return self.dict_id_2_identity[rid]
        except:
            return None


    def load_dict_identity_2_identities(self):
        '''
        Using L{self.dict_identity_adjacency}, populate
        L{self.dict_identity_2_identities}.
        '''
        if not self.dict_identity_adjacency:
            self.fetch_dict_identity_adjacency()
        self.dict_identity_2_identities = {}
        for pair, relationship in self.dict_identity_adjacency.iteritems():
            identity1, identity2 = pair
            if identity1 not in self.dict_identity_2_identities:
                self.dict_identity_2_identities[identity1] = {identity2: relationship}
            else:
                self.dict_identity_2_identities[identity1][identity2] = relationship

            if identity2 not in self.dict_identity_2_identities:
                self.dict_identity_2_identities[identity2] = {identity1: relationship}
            else:
                self.dict_identity_2_identities[identity2][identity1] = relationship




    def getPersons(self, list_identities):
        '''
        For all identities in C{list_identities}, retrieve all associated
        records, add normalized attributes to each record, and for each subset
        with the same identity, instantiate a new L{Person} object.
        Add that object to L{self.dict_persons}.
        '''
        self.dict_persons = {}
        list_all_rids = []
        for identity in list_identities:
            list_all_rids += self.get_ids(identity)

        # Retrieve all relevant records
        print "Retrieving records in getPerson..."
        retriever = FecRetrieverByID(utils.config.MySQL_table_usa_combined)
        retriever.retrieve(list_all_rids)
        dict_of_records = retriever.dict_of_records

        # Load normalized attributes and bind to records
        print "Loading national normalized attributes..."
        filename_normalized_attributes = utils.config.normalized_attributes_file_template % 'USA'
        with open(filename_normalized_attributes) as f:
            dict_normalized_attrs = utils.cPickle.load(f)
        print "Done."
        
        for rid, r in dict_of_records.iteritems():
            r['N_first_name'] = dict_normalized_attrs[rid]['N_first_name']
            r['N_middle_name'] = dict_normalized_attrs[rid]['N_middle_name']
            r['N_last_name'] = dict_normalized_attrs[rid]['N_last_name']
        
        print "Generating Person objects"

        for identity in list_identities:
            list_my_records = [dict_of_records[rid] for rid in self.get_ids(identity)]
            p = Person.Person(list_my_records)
            self.dict_persons[identity] = p
        pass




    def generate_dict_identity_adjacency(self, list_identity_pairs, overwrite=False, export_file = '', verdict_authority = None):
        ''' 
        Compute L{self.dict_identity_adjacency} from a list containing
        results of pairwise S1 identity comparisons. This is a stage2 operation.
        @param list_identity_pairs: a list where each item is of the form
        C{((identity1,identity2), result)}. Each item is then the result of a record
        pair comparison. The tuple C{(identity1, identity2)} is sorted.
        This is a much simpler operation than the v1 method, since now
        L{list_identity_pairs} already contains the final comparison results
        for the S1 identity pairs and no further calculations are required.
        @param verdict_authority: an instance of L{VerdictAuthority}
        that issues a final
        verdict on an element of list_identity_pairs. This object must be supplied
        whether this is a bootstrapping or a disambiguation run. The bootstrapping
        object can be a dummy object with an lambda x:1 as the verdict function. 
        '''
        # TODO: implement! This is just for testing!
        # NOTE: I think this is done.
        print "len of list_identity_pairs:" , len(list_identity_pairs)
        if overwrite:
            dict_identity_adjacency = {}
        for identity_pair, list_results in list_identity_pairs:
            identity1, identity2 = identity_pair
            if identity1 == identity2: 
                #print "Same identity. Skipping."
                continue
            #result_no, result_maybe, result_yes = list_results
            result_name, result_occupation, result_employer = list_results
            
            full_results = [result_name, result_occupation, result_employer]
            dict_identity_adjacency[tuple(sorted([identity1, identity2]))] = full_results


        if export_file != '':
            with open(export_file, 'w') as f:
                for key, value in dict_identity_adjacency.iteritems():
                    # Each line of the file will look like the following:
                    # identity1, identity2, result_name, result_occupation, result_employer
                    data_line  = list(key) + value 
                    f.write(utils.json.dumps(data_line) + "\n")

        # populate self.dict_identity_adjacency using the 
        # supplied verdict function.
        self.dict_identity_adjacency = {key: verdict_authority.verdict(value) for \
                key, value in dict_identity_adjacency.iteritems()}
            
                #f.write('%s %s %s %s %s %s\n' % (identity1, identity2, result_name[1][0], result_name[1][1], result_occupation[1], result_employer[1]))
                #self.dict_identity_adjacency[key] = (result_no, result_maybe, result_yes)




    def generate_dict_identity_adjacency_OLD(self, list_record_pairs, overwrite=False):
        '''
        @deprecated: used for v1. See  L{generate_dict_identity_adjacency}
        for v2.
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
                identity1 = self.get_identity(rid1)
            except KeyError:
                has_identity1 = False

            try:
                identity2 = self.get_identity(rid2)
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




        # Compile a list (set) of all identities associated
        # to records in list_record_pairs
        set_all_identities = set()
        for key, list_results in dict_tmp.iteritems():
            identity1,identity2 = key
            if identity1:
                set_all_identities.add(identity1)
            if identity2:
                set_all_identities.add(identity2)
            
        
        # Get a Person object for each identity
        # TODO: Poulates self.dict_persons
        print "Generating Person objects for all identities..."
        self.getPersons(list(set_all_identities))
        set_all_identities.clear()
        print "Done generating Person objects."


        # Now, go through dict_tmp and for each identity pair
        # interpret all its results and make a final judgment
        # on the relationship between the identities.
        for key, list_results in dict_tmp.iteritems():
            # Loop through all results for this identity pair
            # and update
            result_no = 0
            result_maybe = 0
            result_yes = 0

            # check if the person objects associated
            # with the two identities are compatible.
            # If they are irreconcilable, do not log
            # the pair.
            identity1,identity2 = key
            try:
                person1 = self.dict_persons[identity1]
                person2 = self.dict_persons[identity2]
                mn1, mn2 = person1.get_dominant_attribute('N_middle_name'), person2.get_dominant_attribute('N_middle_name')
                if mn1 and mn2:
                    if mn1[0] != mn2[0]: 
                        result_no += 1
            except:
                print "Error in getting donimant middle names" 
            
            for result in list_results:
                if result < 0:
                    result_no += 1
                elif result == 0:
                    result_maybe += 1
                elif result > 0:
                    result_yes += result

            self.dict_identity_adjacency[key] = (result_no, result_maybe, result_yes)

        print "Done generating identities_adjacency."


    def export_related_identities_csv(self):
        '''
        Export related identities to a csv file. Each line will start
        with a target identity, and all following fields are identities
        related to the target identity. 
        '''
        filename = utils.config.related_identities_template % 'USA'
        if not self.dict_identity_2_identities:
            self.load_dict_identity_2_identities()

        with open(filename,'w') as f:
            for identity, other_identities in self.dict_identity_2_identities.iteritems():
                set_other_identities = set(other_identities.keys())
                set_other_identities.discard(identity)
                set_other_identities = {x for x in set_other_identities if x}
                if set_other_identities:
                    f.write(identity + ' ' +  ' '.join(list(set_other_identities)) + '\n')
            
    def export_linked_identities_csv(self):
        '''
        Export linked identities to a csv file. Each line will start
        with a target identity, and all following fields are identities
        linked to the target identity.
        '''
        filename = utils.config.linked_identities_template % 'USA'
        if not self.dict_identity_2_identities:
            self.load_dict_identity_2_identities()

        with open(filename,'w') as f:
            for identity, other_identities in self.dict_identity_2_identities.iteritems():
                set_other_identities = set(self.get_linked_identities(identity))
                set_other_identities.discard(identity)
                set_other_identities = {x for x in set_other_identities if x}
                if set_other_identities:
                    f.write(identity + ' ' +  ' '.join(list(set_other_identities)) + '\n')

    def export_identities_adjacency(self):
        '''
        Export the contents of L{self.dict_identity_adjacency} to the
        table defined by L{IdentityManager.table_name_identity_adjacency}.
        '''
        print "Exporting identities_adjacency..."
        self.__truncate_table_identities_adjacency()
        print "identities_adjacency truncated successfully."
        for key, verdict in self.dict_identity_adjacency.iteritems():
            # print 'key: ', key
            identity1, identity2 = key
            query = 'INSERT INTO %s (identity1,identity2,verdict) VALUES ("%s", "%s", %d);' \
                   % (IdentityManager.table_name_identity_adjacency, identity1, identity2, verdict)
            print query
            self.runQuery(query)

        self.connection.commit()
#         self.connection.close()
        print "identities_adjacency exported succesfully."






    def drop_table_identities(self):
        '''
        Drop the table "identities" if exists
        '''
        query = "DROP TABLE IF EXISTS %s;" % IdentityManager.table_name_identities
        self.runQuery(query)
        self.connection.commit()



    def drop_table_identities_adjacency(self):
        '''
        Drop the table "identities_adjacency" if exists
        '''
        query = "DROP TABLE IF EXISTS %s;" % IdentityManager.table_name_identity_adjacency
        self.runQuery(query)
        self.connection.commit()



    def __init_table_identities(self, overwrite = False):
        '''
        Check whether the "identities" table exists and
        create it if not.
        @param overwrite: whether or not to overwrite table
        if it already exists.
        '''
        if overwrite:
            print "Dropping table %s" % IdentityManager.table_name_identities
            self.drop_table_identities()


        query = "SELECT COUNT(*) FROM information_schema.tables \
                    WHERE table_schema = 'FEC' \
                    AND table_name = '%s';" % IdentityManager.table_name_identities
        result = self.runQuery(query)
        if result[0][0] == 0:
            print "Table '%s' doesn't exist. Creating it." % IdentityManager.table_name_identities
            self.runQuery(self.query_create_table_identities)
        else:
            print "Table '%s' exists." % IdentityManager.table_name_identities


    def __init_table_identities_adjacency(self, overwrite = False):
        '''
        Check whether the "identities_adjacency" table exists
        and create it if not.
        @param overwrite: whether to overwrite the table if it 
        already exists. 
        '''
        if overwrite:
            print "Dropping table %s" % IdentityManager.table_name_identity_adjacency
            self.drop_table_identities_adjacency()

        query = "SELECT COUNT(*) FROM information_schema.tables \
                    WHERE table_schema = 'FEC' \
                    AND table_name = '%s';" % IdentityManager.table_name_identity_adjacency

        result = self.runQuery(query)
        if result[0][0] == 0:
            print "Table '%s' doesn't exist. Creating it." % IdentityManager.table_name_identity_adjacency
            self.runQuery(self.query_create_table_identities_adjacency)
        else:
            print "Table '%s' exists." % IdentityManager.table_name_identity_adjacency



    def __truncate_table_identities_adjacency(self):
        '''
        Empty the identities_adjacency table.
        @note: Due to a known L{MySQL bug <http://bugs.mysql.com/bug.php?id=68184>},
        the TRUNCATE statement may cause the code to freeze. Instead, we
        drop and re-init the table.
        '''
        self.drop_table_identities_adjacency()
        print "table %s dropped successfully" % IdentityManager.table_name_identity_adjacency 
        self.__init_table_identities_adjacency()
        print "table %s initialized successfully" % IdentityManager.table_name_identity_adjacency 


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
    print utils.config
    quit()

    fr = DatabaseManager(table_name="california", query_fields=["NAME", "CITY", "CMTE_ID", "TRAN_ID"], limit=(1, 1000000), list_order_by=["NAME"])
    fr.retrieve()
    for record in fr.list_of_records:
        print record


