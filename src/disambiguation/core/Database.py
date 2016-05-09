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


def partition_graph_of_reachable_identities(g):
    '''
    Find communities in the graph of reachable identities
    using a spinglass clustering algorightm.
    '''
    gg = g.community_spinglass(weights='weight')
    list_components = [[v['name'] for v in component.vs] for component in gg.subgraphs()]
    return list_components


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



    def __populate_temp_table(self, list_ids, chunk_size=10000):
        '''
        Insert the ids into the temp table.
        @param list_ids: list of record ids tobe retrieved.
        '''
        self.connection.autocommit(False)
        print "inserting..."

        def __enclose_parantheses(list_ids):
            return ','.join(['(%s)' % str(rid) for rid in list_ids])
        for chunk in utils.chunks_size_gen(list_ids, size=10000):
            query = "INSERT INTO %s  values %s;" % (self.temp_table, __enclose_parantheses(chunk))
        # for rid in list_ids:
        #    query = "INSERT INTO %s  value (%d);" % (self.temp_table, rid)
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
        if query_fields == []:
            query_fields = self.all_fields
        self.all_fields = query_fields
        fields = ','.join(self.all_fields)


        if len(list_ids) < 1000:
            list_ids_str = '(%s)' % ','.join([str(rid) for rid in list_ids])
            # Retrieve the rows from the join
            query = "SELECT " + fields + " FROM " + self.table_name + " WHERE id in " + list_ids_str + " ;"
            # print query
            t1 = utils.time.time()
            query_result = self.runQuery(query)
            self.query_result = query_result
            t2 = utils.time.time()
            # print "Done in %f seconds" % (t2 - t1)
        else:
            self.__get_temp_table()

            t1 = utils.time.time()
            self.__populate_temp_table(list_ids)
            t2 = utils.time.time()
            # print "Done in %f seconds" % (t2 - t1)



            # Retrieve the rows from the join
            query = "SELECT " + fields + " FROM " + self.table_name + " JOIN " + self.temp_table + " USING (id) ;"
            # print query
            t1 = utils.time.time()
            query_result = self.runQuery(query)
            self.query_result = query_result
            t2 = utils.time.time()
            # print "Done in %f seconds" % (t2 - t1)

            # Cleanup
            self.__del_temp_table()


        # Convert strings to upper case, dates to date format.
        tmp_list = [[s.upper() if isinstance(s, basestring) else s.strftime("%Y%m%d") if  isinstance(s, datetime.date) else s  for s in record] for record in query_result]
        self.list_results = tmp_list

        self.dict_of_records = {}
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
                r['OCCUPATION'] = r['OCCUPATION'].encode('ascii', 'ignore')
            except:
                pass

            try:
                r['EMPLOYER'] = r['EMPLOYER'].encode('ascii', 'ignore')
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



    def export_csv(self, filename=None, filelabel=None):
        '''
        @param filename
        @param filelabel: DEPRECATED string label for output file. This
        label will be inserted into the csv_exported_state_template
        template.
        '''
        # filename = utils.config.csv_exported_state_template  % filelabel
        if filelabel: filename = filelabel
        self.get_idm()
        list_results_updated = []
        # add identities to records
        index_id = self.all_fields.index('id')
        for line in self.list_results:
            line = list(line)
            r_id = line[index_id]
            identity = self.idm.get_identity(r_id)
            line.insert(0, identity if identity else '')
            list_results_updated.append(line)
        header = ['identity'] + self.all_fields
        df = pd.DataFrame(list_results_updated, columns=header)
        if filename:
            df.to_csv(filename, sep='|', header=header, index=False)
        return df













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


    def retrieve(self, query=''):
        # Get string list from MySQL query and set it as analyst's list_of_records_identifier
        # query_result = runQuery("select " + ','.join(identifier_fields) + " from newyork_addresses where NAME <> '' order by NAME limit " + str(record_start) + "," + str(record_no) + ";")
        if not query:
            query = "select " + ','.join(self.query_fields) + " from " + self.table_name + self.where_clause + self.order_by + self.limit + ";"
            # print query
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

            try:
                r['OCCUPATION'] = r['OCCUPATION'].encode('ascii', 'ignore')
            except:
                pass

            try:
                r['EMPLOYER'] = r['EMPLOYER'].encode('ascii', 'ignore')
            except:
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

        # state
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



    def export_csv(self, filename=None):
        '''
        @param filename: output csv filename. If None, then only
        return the DataFrame.
        @return: the dataframe which was exported
        '''
        # if filename is None:
        #    filename = utils.config.csv_exported_state_template  % self.state
        self.get_idm()
        list_results_updated = []
        # add identities to records
        index_id = self.query_fields.index('id')
        for line in self.list_results:
            line = list(line)
            r_id = line[index_id]
            identity = self.idm.get_identity(r_id)
            line.insert(0, identity if identity else '')
            list_results_updated.append(line)
        header = ['identity'] + self.query_fields
        df = pd.DataFrame(list_results_updated, columns=header)
        if filename:
            df.to_csv(filename, sep='|', header=header, index=False)
        return df











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

    table_name_identities = utils.config.MySQL_table_identities
    table_name_identity_adjacency = utils.config.MySQL_table_identities_adjacency
    table_name_linked_identities = utils.config.MySQL_table_linked_identities


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


        # Dictionary that maps an identity to a list of all others
        # linked to it. This dictionary defines the final "linked"
        # relationship among S1 identities. It is produced after
        # middle name conflict resolution. Singleton identities are
        # not included in this dict.
        self.dict_linked_identities = {}

        # Query that will create table identities
        self.query_create_table_identities = 'CREATE TABLE %s ( id INT PRIMARY KEY, identity VARCHAR(24));' % IdentityManager.table_name_identities

        # Query that will create table identities_adjacency
        self.query_create_table_identities_adjacency = \
            'CREATE TABLE %s (identity1 VARCHAR(24), identity2 VARCHAR(24), verdict INT, PRIMARY KEY (identity1,identity2)  );' % IdentityManager.table_name_identity_adjacency

        # Query that will create table linked_identities
        self.query_create_table_linked_identities = 'CREATE TABLE %s ( identity1 VARCHAR(24), identity2 VARCHAR(24), KEY(identity1) );' % IdentityManager.table_name_linked_identities


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
        self.init_tables(overwrite=False)

        # dict {identity: Person instance}
        self.dict_persons = None


    def init_tables(self, overwrite=False):
        '''
        Initialize the identities and identities_adjacency tables.
        @param overwrite: whether to overwrite the tables if they
        exist already.
        '''
        # Initialize the "identities" and tables
        self.__init_table_identities(overwrite)
        self.__init_table_identities_adjacency(overwrite)
        self.__init_table_linked_identities(overwrite)


    def get_compound_identity(self, rid):
        '''
        Return the compound identity of rid by concatenating
        the identities of all linked identities.
        '''
        identity = self.get_identity(rid)
        if identity is None:
            return '', []
        linked_identities = self.get_linked_identities(identity)

        all_identities = linked_identities + [identity]
        compound_identity = "|".join(sorted(all_identities))
        return compound_identity, all_identities




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


    def get_reachable_identities(self, identity, fcn_linked=None):
        '''
        Return all identities that are reachable from C{identity} via a path of
        edge scores 1 on the graph of related identities.
        This will be done using the function
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
            for neighbor, score in X.items():
                if neighbor not in set_visited and fcn_linked(score):
                    set_visited.add(neighbor)
                    get_neighborhood(neighbor, set_visited)

        set_visited = set([identity])
        get_neighborhood(identity, set_visited)
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




    def generate_dict_identity_adjacency(self, list_identity_pairs, overwrite=False, export_file='', verdict_authority=None):
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
                # print "Same identity. Skipping."
                continue
            # result_no, result_maybe, result_yes = list_results
            result_name, result_occupation, result_employer = list_results

            full_results = [result_name, result_occupation, result_employer]
            dict_identity_adjacency[tuple(sorted([identity1, identity2]))] = full_results


        if export_file != '':
            with open(export_file, 'w') as f:
                for key, value in dict_identity_adjacency.iteritems():
                    # Each line of the file will look like the following:
                    # identity1, identity2, result_name, result_occupation, result_employer
                    data_line = list(key) + value
                    f.write(utils.json.dumps(data_line) + "\n")

        # populate self.dict_identity_adjacency using the
        # supplied verdict function.
        self.dict_identity_adjacency = {key: verdict_authority.verdict(value) for \
                key, value in dict_identity_adjacency.iteritems()}

                # f.write('%s %s %s %s %s %s\n' % (identity1, identity2, result_name[1][0], result_name[1][1], result_occupation[1], result_employer[1]))
                # self.dict_identity_adjacency[key] = (result_no, result_maybe, result_yes)

        # NOTE: at this point we have a dictionary that maps each compared pair to their (-1,0,1)
        # verdict. The question is, how to we derive the final identity clusters from this "edgelist".
        # The challenge is to account for middle name conflicts. If we divide the set of all S1
        # identities into connected components, in some components there will be pairs of identities
        # that have middle name conflicts (score -1). Whenever such a component is found, it must
        # be split up into at least two separate components.
        # Do we do this in export_linked_identities()


    def deduce_linked_identities(self):
        '''
        Generate a set of super-identities, i.e. a sorted tuples of
        S1 identities that are linked and have no middle name conflicts.
        Result is L{self.set_super_identities}
        '''
        dict_super_identities = self.__get_dict_super_identities()
        self.set_super_identities = self.__get_set_super_identities(dict_super_identities)






    def __get_dict_super_identities(self, verbose=False):
        '''
        Compute all the super identities and for each one determine
        if it has internal middle name conflict.
        The key is a sorted tuple of all identities reachable from
        one another.
        The value is an integer counting the number of middle name
        conflicts within the set given the edges for which we have
        comparison results.
        '''

        counter = 0
        counter_error = 0
        conflict = False

        dict_super_identities = {}
        set_super_identities = set()

        for identity, x in self.dict_identity_2_list_ids.iteritems():
            reachable = self.get_reachable_identities(identity)
            set_reachable = set(reachable)
            sorted_reachable = tuple(sorted(reachable))

            set_conflict_pairs = set()

            if sorted_reachable in dict_super_identities:
                continue

            # Register the current tuple of reachables in the
            # dict of super identities. Next, determine if this
            # super identity has a conflict.
            dict_super_identities[sorted_reachable] = 0

            for r_identity in reachable:
                # For each reachable identity, find its related identities.
                dict_related = self.get_related_identities(r_identity)
                if not dict_related: continue


                for other, score in dict_related.iteritems():
                    if other in set_reachable and score == -1:
                        counter_error += 1

                        current_pair = tuple(sorted([r_identity, other]))
                        if current_pair not in set_conflict_pairs:
                            dict_super_identities[sorted_reachable] += 1
                            set_conflict_pairs.add(current_pair)

                        if verbose:
                            print counter_error, " CONFLICT ", len(set_reachable), r_identity, other
                        conflict = True
            counter += 1
        return dict_super_identities

    def __get_set_super_identities(self, dict_super_identities):
        '''
        return a set containing all new super identities.
        This includes all the old ones that didn't require
        partitioning due to middle name conflicts as well
        as new ones resulting from the partitioning of old
        ones with conflicts.
        @param dict_super_identities: a dict where the key
        is a sorted tuple of identities (a super identity)
        and the value is the number of middle name conflicts
        detected within the super identity.
        @return: set where each item is a sorted tuple of
        self.identities.
        '''

        counter = 0

        # The new identities being born including the
        # old ones that didn't need partitioning.
        birth_row = set()

        for key, value in dict_super_identities.iteritems():
            if value == 0 :
                birth_row.add(key)
                continue

            # The the graph of reachable identities
            g = self.get_reachable_identities_graph(key[0])
            list_components = partition_graph_of_reachable_identities(g)
            for component in list_components:
                birth_row.add(tuple(sorted(component)))

            counter += 1
            print "Conflict resolved   ", counter

        return birth_row





    def get_reachable_identities_graph(self, identity):
        '''
        Return a graph of all identities that are reachable from
        the given identity on the graph of related identities.
        Edge 'weight' can be -1, 0, 1.
        '''

        dict_weights = {-1:-100}
        def func_weight(verdict):
            '''weight = verdict, unless verdict is one
            of the values defined in dict_weights.'''
            try:
                weight = dict_weights[verdict]
            except:
                weight = verdict
            return weight

        edgelist = {}

        set_reachable_identities = set(self.get_reachable_identities(identity))

        for neighbor in set_reachable_identities:
            dict_related = self.get_related_identities(neighbor)
            for related, verdict in dict_related.iteritems():
                if verdict < 0 and related not in set_reachable_identities:
                    continue
                edge = tuple(sorted([neighbor, related]))
                edgelist[edge] = func_weight(verdict)
        edgelist = [(key[0], key[1], value) for key, value in edgelist.iteritems()]
        g = utils.igraph.Graph.TupleList(edgelist, weights=True)
        return g



    def get_linked_identities(self, identity):
        '''
        Using the table C{linked_identities}, return a list of all other
        identities linked to C{identity}.
        In previous versions, we did this just by using L{self.dict_identities_adjacency}
        but now, that is not enough. We must use the contents of C{linked_identities}
        which are the result of further processing of the said dict, after resolving
        middle name conflicts and splitting some of the super identities.
        '''
        if not self.dict_linked_identities:
            print "dict_linked_identities not loaded. Loading now..."
            self.fetch_dict_linked_identities()

        try:
            return self.dict_linked_identities[identity]
        except:
            return []



    def export_related_identities_csv(self):
        '''
        Export related identities to a csv file. Each line will start
        with a target identity, and all following fields are identities
        related to the target identity.
        '''
        filename = utils.config.related_identities_template % 'USA'
        if not self.dict_identity_2_identities:
            self.load_dict_identity_2_identities()

        with open(filename, 'w') as f:
            for identity, other_identities in self.dict_identity_2_identities.iteritems():
                set_other_identities = set(other_identities.keys())
                set_other_identities.discard(identity)
                set_other_identities = {x for x in set_other_identities if x}
                if set_other_identities:
                    f.write(identity + ' ' + ' '.join(list(set_other_identities)) + '\n')

    def export_linked_identities_csv(self):
        '''
        Export linked identities to a csv file. Each line will start
        with a target identity, and all following fields are identities
        linked to the target identity.
        '''
        filename = utils.config.linked_identities_template % 'USA'
        if not self.dict_identity_2_identities:
            self.load_dict_identity_2_identities()

        with open(filename, 'w') as f:
            for identity, other_identities in self.dict_identity_2_identities.iteritems():
                set_other_identities = set(self.get_linked_identities(identity))
                set_other_identities.discard(identity)
                set_other_identities = {x for x in set_other_identities if x}
                if set_other_identities:
                    f.write(identity + ' ' + ' '.join(list(set_other_identities)) + '\n')

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




    def export_linked_identities(self):
        '''
        Using the contents of L{self.set_super_identities}, populate the
        table defined by L{IdentityManager.table_name_identity_adjacency}.
        '''

        if not self.set_super_identities:
            raise Exception('self.set_super_identities is empty. Cannot export empty set')

        print "Exporting linked_identities..."
        self.__truncate_table_linked_identities()
        print "linked_identities truncated successfully."


        self.connection.autocommit(False)
        counter = 0
        list_values = []
        for super_identity in self.set_super_identities:
            for identity1 in super_identity:
                for identity2 in super_identity:
                    if identity1 == identity2: continue
                    counter += 1

                    list_values.append((identity1, identity2))
                    if len(list_values) == 10000:
                        list_values_str = ",".join(['("%s", "%s")' % values for values in list_values])
                        query = 'INSERT INTO %s (identity1,identity2) VALUES %s;' \
                       % (IdentityManager.table_name_linked_identities, list_values_str)
                        self.runQuery(query)
                        list_values = []
        if list_values:
            list_values_str = ", ".join(['("%s", "%s")' % values for values in list_values])
            query = 'INSERT INTO %s (identity1,identity2) VALUES %s;' \
               % (IdentityManager.table_name_linked_identities, list_values_str)
            self.runQuery(query)
            list_values = []

        self.connection.commit()
        self.connection.autocommit(True)

#         self.connection.close()
        print "linked_identities exported succesfully."
        print "Total number of entries: ", counter



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


    def drop_table_linked_identities(self):
        '''
        Drop the table "linked_identities" if exists
        '''
        query = "DROP TABLE IF EXISTS %s;" % IdentityManager.table_name_linked_identities
        self.runQuery(query)
        self.connection.commit()


    def __init_table_identities(self, overwrite=False):
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


    def __init_table_identities_adjacency(self, overwrite=False):
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


    def __init_table_linked_identities(self, overwrite=False):
        '''
        Check whether the "linked_identities" table exists
        and create it if not.
        @param overwrite: whether to overwrite the table if it
        already exists.
        '''
        if overwrite:
            print "Dropping table %s" % IdentityManager.table_name_linked_identities
            # TODO: implement
            self.drop_table_linked_identities()

        query = "SELECT COUNT(*) FROM information_schema.tables \
                    WHERE table_schema = 'FEC' \
                    AND table_name = '%s';" % IdentityManager.table_name_linked_identities

        result = self.runQuery(query)
        if result[0][0] == 0:
            print "Table '%s' doesn't exist. Creating it." % IdentityManager.table_name_linked_identities
            self.runQuery(self.query_create_table_linked_identities)
        else:
            print "Table '%s' exists." % IdentityManager.table_name_linked_identities



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


    def __truncate_table_linked_identities(self):
        '''
        Empty the linked_identities table.
        @note: Due to a known L{MySQL bug <http://bugs.mysql.com/bug.php?id=68184>},
        the TRUNCATE statement may cause the code to freeze. Instead, we
        drop and re-init the table.
        '''
        self.drop_table_linked_identities()
        print "table %s dropped successfully" % IdentityManager.table_name_linked_identities
        self.__init_table_linked_identities()
        print "table %s initialized successfully" % IdentityManager.table_name_linked_identities


    def fetch_dict_linked_identities(self):
        '''
        Fetch L{self.dict_linked_identities} from the C{linked_identities}
        table. This dict contains the final data on identities that are
        linked by our algorithm.
        '''
        query = "select identity1, identity2 from " + IdentityManager.table_name_linked_identities + ";"
        print query
        self.query = query
        query_result = self.runQuery(query)

        for identity1, identity2 in query_result:
            try:
                self.dict_linked_identities[identity1].append(identity2)
            except KeyError:
                self.dict_linked_identities[identity1] = [identity2]
        del query_result


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


