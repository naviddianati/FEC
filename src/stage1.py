
import disambiguation.init as init

from disambiguation.core import Project, utils
from disambiguation.core.Affiliations import AffiliationAnalyzerUndirected, MigrationAnalyzerUndirected
from disambiguation.core.Database import FecRetriever
import disambiguation.core.Disambiguator as Disambiguator
from disambiguation.core.Tokenizer import Tokenizer, TokenizerNgram, TokenData
import disambiguation.config as config
from disambiguation.core.states import *
from disambiguation.core.utils import *
import resource
from copy import copy







def disambiguate_main(state, record_limit=(0, 5000000), method_id="thorough", logstats=False, whereclause='', num_procs=1, percent_employers=5, percent_occupations=5):
    '''
    For the given state, load all records, the affiliation graphs, and disambiguate the records using the specified
    number of processes.
    
    Steps:
        1. Pick a list of fields, pick a table and instantiate an FecRetriever object to fetch those fields from the table.
        This produces a list of Record objects.
        2. Instantiate a Tokenizer object, and pass the list of records to the Tokenizer. Tokenize them, and retrieve the
        updated list of records. These records now have a reference to the Tokenizers TokenData object, and contain
        an attribute called record.vector.
        3. Instantiate a Disambiguator object and pass to it the list of records. This Disambiguator will use the vector
        attributes of the records to find a set of approximate nearest neighbors for each one. The result is an adjacency
        matrix.
        4. Instantiate a Project object, and set various parameters to it as instance variables. For example, the Disambiguator
        object defined above is bound to the Project as an instance variable.
        This Project object will then do the book keeping: saves a settings file, saves the adjacency matrix to a file,
        saves a json version of all records to a file, etc.
        5. The json files saved by Project, namely the adjacency matrix and the list of records, will be used by the code
        defined in the Affiliations module to extract
        
    @param state: The state to disambiguate. Must be the full name, all lowercase.
    @param record_limit: a tuple C{(start, number)} specifying the records to retrieve from db. 
    @param method_id: The record comparison method to use for matching records. Multiple methods
    are avaliable, but for our current purpose, i.e., stage 1 statewide disambiguation, we use the
    "thorough" method.
    @param logstats: Whether or not to log statistics about mathces.
    @param whereclause: whereclasue to use with the MySQL query.
    @param percent_employers: percentage of top edges in the employers network to use.
    @param percent_occupations: percentage of top edges in the occupations network to use.

    '''
    batch_id = get_next_batch_id()
    project = Project.Project(batch_id=batch_id)

    if state:
        param_state = state
    else:
        param_state = 'newyork'
    print "Analyzing data for state: ", param_state

    table_name = param_state + "_combined"

    project.putData('state' , param_state)




    record_start = record_limit[0]
    record_no = record_limit[1]

    project.putData('batch_id' , batch_id)

    time1 = time.time()

#    list_tokenized_fields = ['NAME','FIRST_NAME', 'LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
    list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    project.putData("list_tokenized_fields", list_tokenized_fields)

    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)

    all_fields = list_tokenized_fields + list_auxiliary_fields
    project.putData('all_fields' , all_fields)

    # I BELIEVE THESE ARE UNNECCESSARY
    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]

    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]


    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.putData("index_2_field", index_2_field)

    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.putData("field_2_index", field_2_index)


    if whereclause == "":
        pass
        # Only disambiguate "IND" types
        # whereclause = " WHERE ENTITY_TP='IND' "
    else:
        pass
        # whereclause = whereclause + " AND ENTITY_TP='IND' "
        print "whereclasue: ", whereclause

    print_resource_usage('---------------- before retrieving data')
    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      where_clause=whereclause
                      )
    retriever.retrieve()
    project.putData("query", retriever.getQuery())

    print_resource_usage('---------------- after retrieving data')
    list_of_records = retriever.getRecords()
    print_resource_usage('---------------- after setting list_of_records')
    if not list_of_records:
        print "ERROR: list of records empty. Aborting..."
        return


    # Get tokendata and make sure vectors are exported to file.
    tokenizer, list_of_records = init.tokenize_records(list_of_records, project, TokenizerNgram)
    tokendata = tokenizer.tokens



    '''
    Load affiliation graph data.
    These graph objects also contain a dictionary G.dict_string_2_ind
    for faster vertex access based on affiliation string.
    For affiliation data to exist, one must have previously executed
    When used via self.compare(), a positive value is
    interpreted as True.   Affiliations on the <state>_addresses data.
    '''

    G_employer = utils.loadAffiliationNetwork(param_state , 'employer', percent=percent_employers)
    if G_employer:
        for record in list_of_records:
            record.list_G_employer = [G_employer]
    else:
        print "WARNING: EMPLOYER network not found."
        project.log('WARNING', param_state + " EMPLOYER network not found.")



    G_occupation = utils.loadAffiliationNetwork(param_state , 'occupation', percent=percent_occupations)
    if G_occupation:
        for record in list_of_records:
            record.list_G_occupation = [G_occupation]
    else:
        print "WARNING: OCCUPATION network not found."
        project.log('WARNING', param_state + " OCCUPATION network not found.")


    print_resource_usage('---------------- after loading affiliation networks')






    project.list_of_records = list_of_records
    print_resource_usage('---------------- after assigning list_of_records to project')

    # dimension of input vectors
    dim = tokendata.no_of_tokens

    D = Disambiguator.Disambiguator(list_of_records, dim, matching_mode=method_id, num_procs=num_procs)
#     D.tokenizer = tokenizer
    project.D = D
    D.project = project
    D.tokenizer = tokenizer

    # Set D's logstats on or off
    D.set_logstats(is_on=logstats)
    print_resource_usage('---------------- after defining Disambiguator')


    # desired dimension (length) of hashes
    hash_dim = 20
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 40


    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))


#     self.D = Disambiguator(self.list_of_vectors, self.tokens.index_2_token, self.tokens.token_2_index, dim, self.batch_id)


    print 'Analyze...'
    t1 = time.time()
    # compute the hashes
    D.get_LSH_hashes(hash_dim, num_procs=num_procs)
    print_resource_usage('---------------- after get_LSH_hashes')

#     return project


    D.compute_similarity(B1=B, m=no_of_permutations , sigma1=None)
    print_resource_usage('---------------- After compute_similarity')

#     project.save_data_textual(with_tokens=False, file_label="before")

    D.generate_identities()
    print_resource_usage('---------------- after generate_identities')

    D.refine_identities()
    print_resource_usage('---------------- after refine_identities')
    # If logstats was on, rename stats.txt to include param_state
    if logstats:
        try:
            os.rename(D.logstats_filename, 'stats-' + param_state + ".txt")
        except Exception as e:
            print e
        D.set_logstats(is_on=False)



#     print json.dumps(D.dict_already_compared_pairs.keys(),indent=2)
#     quit()
    # D.show_sample_adjacency()

    t2 = time.time()
    print 'Done...'
    print t2 - t1

    # project.D.imshow_adjacency_matrix(r=(0, record_no(

    print 'Saving adjacency matrix to file...'
    print 'Printing list of identifiers and text of adjacency matrix to file...'
    project.save_data(with_tokens=False)
    project.save_data_textual(with_tokens=False, file_label=param_state + "-" + "disambiguation-")
    print 'Done...'

    time2 = time.time()
    print time2 - time1

    project.saveSettings(file_label=param_state + "-" + "disambiguation-")

    project.dump_full_adjacency(file_label=param_state + "-" + "disambiguation")

    print_resource_usage('---------------- after saving output data.')


    return project










def generateAffiliationData(state=None, affiliation=None, record_limit=(0, 5000000), whereclause='', num_procs=1):
    '''
    One version of main().
    Operates on <state>_addresses tables in order to find
    high-certainty identities so that affiliation networks
    can be generated. The output of this method will be the
    data files which can be loaded by loadAffiliationNetwork().
    The comparison method used by Records here should be
    strict_address.
    
    Steps: 
        1. Pick a list of fields, pick a table and instantiate an FecRetriever object to fetch those fields from the table.
        This produces a list of Record objects.
        2. Instantiate a Tokenizer object, and pass the list of records to the Tokenizer. Tokenize them, and retrieve the
        updated list of records. These records now have a reference to the Tokenizers TokenData object, and contain
        an attribute called record.vector.
        3. Instantiate a Disambiguator object and pass to it the list of records. This Disambiguator will use the vector
        attributes of the records to find a set of approximate nearest neighbors for each one. The result is an adjacency
        matrix.
        4. Instantiate a Project object, and set various parameters to it as instance variables. For example, the Disambiguator
        object defined above is bound to the Project as an instance variable.
        This Project object will then do the book keeping: saves a settings file, saves the adjacency matrix to a file,
        saves a json version of all records to a file, etc.
        5. The json files saved by Project, namely the adjacency matrix and the list of records, will be used by the code
        defined in the Affiliations module to extract
    '''
    batch_id = get_next_batch_id()
    project = Project.Project(batch_id=batch_id)

    if state:

        param_state = state
    else:
        param_state = 'newyork_addresses'

    print "Analyzing data for state: ", param_state

    table_name = param_state + "_combined"

    # If this is for a multi-state table
    if param_state == "multi_state":
        table_name = "multi_state_combined"
    project.putData('state' , param_state)


    record_start = record_limit[0]
    record_no = record_limit[1]


    project.putData('batch_id' , batch_id)


    time1 = time.time()

#    list_tokenized_fields = ['NAME','FIRST_NAME', 'LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    project.putData("list_tokenized_fields", list_tokenized_fields)

    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)

    all_fields = list_tokenized_fields + list_auxiliary_fields
    project.putData('all_fields' , all_fields)


    # I BELIEVE THESE ARE UNNECCESSARY
    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]

    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]


    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.putData("index_2_field", index_2_field)

    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.putData("field_2_index", field_2_index)


    if whereclause == "":
        whereclause = " WHERE ENTITY_TP='IND' "
    else:
        whereclause = whereclause + " AND ENTITY_TP='IND' "
        print "whereclasue: ", whereclause

    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE", "CMTE_ID"],
                      where_clause=whereclause)
    retriever.retrieve()
    project.putData("query", retriever.getQuery())

    list_of_records = retriever.getRecords()

#     # A list(1) of lists(2)  where each list(2) corresponds to one of the records returned by MySQL
#     # and it contains only the "identifier" fields of that record. This is the main piece of data processed by this program.
#     list_of_records_identifier = [[record[index] for index in index_list_tokenized_fields] for record in tmp_list]
#
#     # Same as above, except each list(2) consists of the auxiliary columns of the record returned by MySQL.
#     list_of_records_auxiliary = [[record[index] for index in index_auxiliary_fields] for record in tmp_list]






    tokenizer, list_of_records = init.tokenize_records(list_of_records, project, TokenizerNgram)
    tokendata = tokenizer.tokens
    ''' HERE WE DON'T LOAD AFFILIATION DATA BECAUSE THAT'S WHAT WE WANT TO PRODUCE! '''


    # Unnecessary
#     project.list_of_records = list_of_records

    # dimension of input vectors
    dim = tokendata.no_of_tokens

    D = Disambiguator.Disambiguator(list_of_records, dim, matching_mode="strict_address", num_procs=num_procs)
    project.D = D
    D.project = project


    # desired dimension (length) of hashes
    hash_dim = 20
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 40


    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))

    print 'Analyze...'
    t1 = time.time()
    # compute the hashes
    D.get_LSH_hashes(hash_dim)

    D.compute_similarity(B1=B, m=no_of_permutations , sigma1=None)

    # D.show_sample_adjacency()

    t2 = time.time()
    print 'Done...'
    print t2 - t1

    # project.D.imshow_adjacency_matrix(r=(0, record_no))

    print 'Saving adjacency matrix to file...'
    print 'Printing list of identifiers and text of adjacency matrix to file...'
    project.save_data(with_tokens=False, file_label=param_state + "-" + "affiliations-")
    print 'Done...'

    time2 = time.time()
    print time2 - time1




    project.log('MESSAGE', 'Computing affiliation networks...')
    project.saveSettings(file_label=param_state + "-" + "affiliations-")

    if affiliation is None or affiliation == 'occupation':
        try:
            analyst = AffiliationAnalyzerUndirected(state=param_state, batch_id=batch_id, affiliation="occupation")
            project.log('MESSAGE', 'AffiliationAnalyzer created...')

            state = analyst.settings["state"]
            analyst.load_data()
            analyst.extract()
            analyst.compute_affiliation_links()
            project.log('MESSAGE', 'Affiliation links computed...')

            analyst.save_data(label=state)
            project.log('MESSAGE', 'Affiliation data saved...')
        #     analyst.save_data_textual(label=state)
        except IndexError:
            pass
#         except Exception as e:
#             print e.msg


    if affiliation is None or affiliation == 'employer':
        try:
            analyst = AffiliationAnalyzerUndirected(state=param_state, batch_id=batch_id, affiliation="employer")
            project.log('MESSAGE', 'AffiliationAnalyzer created...')

            state = analyst.settings["state"]
            analyst.load_data()
            analyst.extract()
            analyst.compute_affiliation_links()
            project.log('MESSAGE', 'Affiliation links computed...')

            analyst.save_data(label=state)
            project.log('MESSAGE', 'Affiliation data saved...')
        except Exception as e:
            print "ERROR", e.msg






def generateMigrationData(state=None, affiliation=None, record_limit=(0, 5000000), whereclause='', num_procs=3):
    '''
    1- Pick a list of fields, pick a table and instantiate an FecRetriever object to fetch those fields from the table.
    This produces a list of Record objects.
    2- Instantiate a Tokenizer object, and pass the list of records to the Tokenizer. Tokenize them, and retrieve the
    updated list of records. These records now have a reference to the Tokenizers TokenData object, and contain
    an attribute called record.vector.
    3- Instantiate a Disambiguator object and pass to it the list of records. This Disambiguator will use the vector
    attributes of the records to find a set of approximate nearest neighbors for each one. The result is an adjacency
    matrix.
    4- Instantiate a Project object, and set various parameters to it as instance variables. For example, the Disambiguator
    object defined above is bound to the Project as an instance variable.
    This Project object will then do the book keeping: saves a settings file, saves the adjacency matrix to a file,
    saves a json version of all records to a file, etc.
    5- The json files saved by Project, namely the adjacency matrix and the list of records, will be used by the code
    defined in the Affiliations module to extract
    '''
    batch_id = get_next_batch_id()
    project = Project.Project(batch_id=batch_id)

    if state:

        param_state = state
    else:
        param_state = 'newyork_addresses'

    print "Analyzing data for state: ", param_state

    table_name = param_state + "_combined"

    # If this is for a multi-state table
    if param_state == "multi_state":
        table_name = "multi_state_combined"
    project.putData('state' , param_state)


    record_start = record_limit[0]
    record_no = record_limit[1]


    project.putData('batch_id' , batch_id)


    time1 = time.time()

#    list_tokenized_fields = ['NAME','FIRST_NAME', 'LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    project.putData("list_tokenized_fields", list_tokenized_fields)

    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)

    all_fields = list_tokenized_fields + list_auxiliary_fields
    project.putData('all_fields' , all_fields)


    # I BELIEVE THESE ARE UNNECCESSARY
    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]

    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]


    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.putData("index_2_field", index_2_field)

    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.putData("field_2_index", field_2_index)


    if whereclause == "":
        whereclause = " WHERE ENTITY_TP='IND' "
    else:
        whereclause = whereclause + " AND ENTITY_TP='IND' "
        print "whereclasue: ", whereclause

    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE", "CMTE_ID"],
                      where_clause=whereclause)
    retriever.retrieve()
    project.putData("query", retriever.getQuery())

    list_of_records = retriever.getRecords()

#     # A list(1) of lists(2)  where each list(2) corresponds to one of the records returned by MySQL
#     # and it contains only the "identifier" fields of that record. This is the main piece of data processed by this program.
#     list_of_records_identifier = [[record[index] for index in index_list_tokenized_fields] for record in tmp_list]
#
#     # Same as above, except each list(2) consists of the auxiliary columns of the record returned by MySQL.
#     list_of_records_auxiliary = [[record[index] for index in index_auxiliary_fields] for record in tmp_list]

    tokenizer, list_of_records = init.tokenize_records(list_of_records, project, TokenizerNgram)
    tokendata = tokenizer.tokens



    # Unnecessary
#     project.list_of_records = list_of_records

    # dimension of input vectors
    dim = tokendata.no_of_tokens

    D = Disambiguator.Disambiguator(list_of_records, dim, matching_mode="strict_affiliation", num_procs=num_procs)
    project.D = D
    D.project = project


    # desired dimension (length) of hashes
    hash_dim = 20
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 40


    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))

    print 'Analyze...'
    t1 = time.time()
    # compute the hashes
    D.get_LSH_hashes(hash_dim)
    D.compute_similarity(B1=B, m=no_of_permutations , sigma1=None)

    # D.show_sample_adjacency()

    t2 = time.time()
    print 'Done...'
    print t2 - t1

    # project.D.imshow_adjacency_matrix(r=(0, record_no))

    print 'Saving adjacency matrix to file...'
    print 'Printing list of identifiers and text of adjacency matrix to file...'
    project.save_data(with_tokens=False, file_label=param_state + "-" + "affiliations-")
    print 'Done...'

    time2 = time.time()
    print time2 - time1




    project.log('MESSAGE', 'Computing affiliation networks...')
    project.saveSettings(file_label=param_state + "-" + "affiliations-")

    if affiliation is None or affiliation == 'zip_code':
        try:
            analyst = MigrationAnalyzerUndirected(state=param_state, batch_id=batch_id, affiliation="zip_code")
            project.log('MESSAGE', 'AffiliationAnalyzer created...')

            state = analyst.settings["state"]
            analyst.load_data()
            analyst.extract()
            analyst.compute_affiliation_links()
            project.log('MESSAGE', 'Affiliation links computed...')

            analyst.save_data(label=state)
            project.log('MESSAGE', 'Affiliation data saved...')
        #     analyst.save_data_textual(label=state)
        except IndexError:
            print "INDEX ERROR"
            pass










def hand_code(state, record_limit=(0, 5000000), sample_size="10000", method_id="thorough", logstats=False):
    '''
    1- Pick a list of fields, pick a table and instantiate an FecRetriever object to fetch those fields from the table.
    This produces a list of Record objects.
    2- Instantiate a Tokenizer object, and pass the list of records to the Tokenizer. Tokenize them, and retrieve the
    updated list of records. These records now have a reference to the Tokenizers TokenData object, and contain
    an attribute called record.vector.
    3- Instantiate a Disambiguator object and pass to it the list of records. This Disambiguator will use the vector
    attributes of the records to find a set of approximate nearest neighbors for each one. The result is an adjacency
    matrix.
    4- Instantiate a Project object, and set various parameters to it as instance variables. For example, the Disambiguator
    object defined above is bound to the Project as an instance variable.
    This Project object will then do the book keeping: saves a settings file, saves the adjacency matrix to a file,
    saves a json version of all records to a file, etc.
    5- The json files saved by Project, namely the adjacency matrix and the list of records, will be used by the code
    defined in the Affiliations module to extract
    '''
    batch_id = get_next_batch_id()
    project = Project.Project(batch_id=batch_id)

    if state:
        param_state = state
    else:
        # TODO: this is the table name. Which table should be used for the final disambiguation?
        # Answer: for the state level, <state>_combined. For the whole country, probably the union
        # of all state tables.

        param_state = 'newyork_combined'
    print "Analyzing data for state: ", param_state

    table_name = param_state + "_combined"

    project.putData('param_state' , param_state)




    record_start = record_limit[0]
    record_no = record_limit[1]

    project.putData('batch_id' , batch_id)


    time1 = time.time()

#    list_tokenized_fields = ['NAME','FIRST_NAME', 'LAST_NAME', 'CONTRIBUTOR_ZIP', 'CONTRIBUTOR_STREET_1']
    list_tokenized_fields = ['NAME', 'TRANSACTION_DT', 'ZIP_CODE' , 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    project.putData("list_tokenized_fields", list_tokenized_fields)

    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)

    all_fields = list_tokenized_fields + list_auxiliary_fields
    project.putData('all_fields' , all_fields)


    # I BELIEVE THESE ARE UNNECCESSARY
    # Where in the final query_fields is the given identifier field?
    index_list_tokenized_fields = [all_fields.index(s) for s in list_tokenized_fields]

    # Where in the final query_fields is the given auxiliary field?
    index_auxiliary_fields = [all_fields.index(s) for s in list_auxiliary_fields]


    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.putData("index_2_field", index_2_field)

    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.putData("field_2_index", field_2_index)




    retriever = FecRetriever(table_name=table_name,
                      query_fields=all_fields,
                      limit=(record_start, record_no),
                      list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE"],
                      where_clause=" WHERE NAME LIKE '%COHEN, ROBERT%'")
#                       where_clause=" ")
    retriever.retrieve()
    project.putData("query", retriever.getQuery())

    list_of_records = retriever.getRecords()
    list_of_records.sort(cmp=lambda x, y: np.random.randint(-1, 2))

    set_records = set(list_of_records)

    list_of_records = []

    counter = 0
    while set_records and counter < sample_size:
        record = set_records.pop()
        list_of_records.append(record)
        counter += 1


    # Pick a random sample of the retrieved records


#     # A list(1) of lists(2)  where each list(2) corresponds to one of the records returned by MySQL
#     # and it contains only the "identifier" fields of that record. This is the main piece of data processed by this program.
#     list_of_records_identifier = [[record[index] for index in index_list_tokenized_fields] for record in tmp_list]
#
#     # Same as above, except each list(2) consists of the auxiliary columns of the record returned by MySQL.
#     list_of_records_auxiliary = [[record[index] for index in index_auxiliary_fields] for record in tmp_list]


    tokenizer = TokenizerNgram()
    project.tokenizer = tokenizer
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(list_tokenized_fields)
    tokenizer.tokenize()

    tokenizer.tokens.save_to_file(project["data_path"] + param_state + "-" + "disambiguate-" + batch_id + "-tokendata.pickle")
    list_of_records = tokenizer.getRecords()



    ''' Load affiliation graph data.
    These graph objects also contain a dictionary G.dict_string_2_ind
    for faster vertex access based on affiliation string.

    For affiliation data to exist, one must have previously executed
    When used via self.compare(), a positive value is
    interpreted as True.   Affiliations on the <state>_addresses data.
    '''

    G_employer = utils.loadAffiliationNetwork(param_state, 'employer')
    if G_employer:
        for record in list_of_records:
            record.list_G_employer = [G_employer]
        print "G_employer loaded"
    else:
        print "WARNING: EMPLOYER network not found."
        project.log('WARNING', param_state + " EMPLOYER network not found.")



    G_occupation = utils.loadAffiliationNetwork(param_state, 'occupation')
    if G_occupation:
        for record in list_of_records:
            record.list_G_occupation = [G_occupation]
        print "G_occupation loaded"
    else:
        print "WARNING: OCCUPATION network not found."
        project.log('WARNING', param_state + " OCCUPATION network not found.")







    n = len(list_of_records)
    counter = 0

    file_handcode_data = open("handcode-data.txt", 'a', 0)
    file_handcode_log = open("handcode-log.txt", 'a', 0)
    while counter < 10:
        i1 = np.random.randint(0, n);
        i2 = np.random.randint(0, n);

        r1 = list_of_records[i1]
        r2 = list_of_records[i2]

        if i1 == i2 : continue

        if r1['N_last_name'] != r2['N_last_name'] : continue
        if r1['N_first_name'] != r2['N_first_name'] : continue






        data1 = [ r1['NAME'], r1['CITY'], r1['ZIP_CODE'], r1['EMPLOYER'], r1['OCCUPATION'], r1['N_middle_name']]
        data2 = [ r2['NAME'], r2['CITY'], r2['ZIP_CODE'], r2['EMPLOYER'], r2['OCCUPATION'], r2['N_middle_name']]

        # We're only accepting records with the same first names and the same last names.
        pvalue = np.log(r1.get_name_pvalue(which="firstname") * r1.get_name_pvalue(which="lastname"))
        output = pd.DataFrame([data1, data2]).to_string()
        result = r1.compare(r2, mode="thorough")
        verdict = result[0]

        if result[1]['o'] != 1: continue
#         if verdict: continue;

        print output
        print pvalue
        file_handcode_log.write(output + "\n")



        print verdict, result[1]
        me = raw_input()
        print "_"*80
        file_handcode_log.write(me + "\n" + str(verdict) + "   " + str(result[1]) + "\n" + "_"*80 + "\n")


        if not verdict:
            verdict = 0

        if me == 'a':
            me = 1
        elif me == '':
            me = 'NaN'
        else:
            me = 0

        file_handcode_data.write("%s %s\n" % (str(int(verdict)), str(me)))



    file_handcode_data.close()

    return


    print len(tokenizer.tokens.token_2_index.keys())


    project.list_of_records = list_of_records

    # dimension of input vectors
    dim = tokenizer.tokens.no_of_tokens

    D = Disambiguator.Disambiguator(list_of_records, dim, matching_mode=method_id)
    D.tokenizer = tokenizer
    project.D = D
    D.project = project

    # Set D's logstats on or off
    D.set_logstats(is_on=logstats)

    # desired dimension (length) of hashes
    hash_dim = 40
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 40

    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))


    print 'Analyze...'
    t1 = time.time()
    # compute the hashes
    D.get_LSH_hashes(hash_dim)
    D.save_LSH_hash(batch_id=batch_id)
    D.compute_similarity(B1=B, m=no_of_permutations , sigma1=None)

#     project.save_data_textual(with_tokens=False, file_label="before")

    D.generate_identities()
    D.refine_identities()

    # If logstats was on, rename stats.txt to include param_state
    if logstats:
        try:
            os.rename('stats.txt', 'stats-' + param_state + ".txt")
        except Exception as e:
            print e
        D.set_logstats(is_on=False)



    # D.show_sample_adjacency()

    t2 = time.time()
    print 'Done...'
    print t2 - t1

    # project.D.imshow_adjacency_matrix(r=(0, record_no(

    print 'Saving adjacency matrix to file...'
    print 'Printing list of identifiers and text of adjacency matrix to file...'
#     project.save_data(with_tokens=False)
    project.save_data_textual(with_tokens=False, file_label=param_state)
    print 'Done...'

    time2 = time.time()
    print time2 - time1

    project.saveSettings(file_label=param_state + "-" + "affiliations-")

    project.dump_full_adjacency()
    return project







def process_last_names(project):
    D = project.D
    f = open('multipele_last_names.txt', 'w')
    counter = 0
    for person in D.set_of_persons:
        all_names = set()
        for record in person.set_of_records:
            all_names.add(record['N_last_name'])
        if len(all_names) > 1:
            counter += 1
            f.write(person.toString() + "\n")
            print "_" * 80
    f.close()
    print "Total number of persons containing different last names: ", counter




def process_middle_names(project):
    D = project.D
    dict_middle_names = {}
    for record in D.list_of_records:
        if not record['N_middle_name']: continue
        try:
            dict_middle_names[record['N_middle_name']] += 1
        except KeyError:
            dict_middle_names[record['N_middle_name']] = 1
    sorted_list = [(name, freq) for name, freq in dict_middle_names.iteritems()]
    sorted_list.sort(key=lambda x: x[1])

    f = open('middle_name_freqs.txt', 'w')
    for item in sorted_list:
        f.write('%s\t%d\n' % item)
    f.close()


def print_resource_usage(msg):
    '''
    Print resource usage.
    '''
    print msg, "\n      " , resource.getrusage(resource.RUSAGE_SELF).ru_maxrss










def worker_disambiguate_states(conn):
    '''
    Worker function that receives a list of states and
    runs C{disambiguate_main} for each one.
    '''
    data = conn.recv()
    proc_name = multiprocessing.current_process().name
    print proc_name, data

    list_results = []

    for state in data:
        # try:
        project = disambiguate_main(state, record_limit=(0, 500000000))

        try:
            project.D.save_identities_to_db(overwrite = True)
        except Exception as e:
            raise

        # except Exception as e:
        #    print "ERROR: Could not disambiguate state ", state, ":   ", e




def disambiguate_multiple_states(list_states=[], num_procs=12):
    '''
    Using multiple processes, disambiguate multiple states. Each process will run
    the worker function L{worker_disambiguate_states}
    @param list_states: list of state strings. If empty, disambiguate all states.
    @param num_procs: number of processes to use.
    '''
    # Use custom list of states
    # list_states = ['virginia', 'maryland']
    # list_states = ['california', 'texas', 'marshallislands', 'palau', 'georgia', 'newjersey']
    # list_states = ['alaska', 'delaware']

    list_jobs = []

    # if not specified,  load all states
    if not list_states:
        list_states = get_states_sorted()
        list_states.reverse()


    N = len(list_states)

    # No more than num_procs processes
    number_of_processes = min(N, num_procs)


    dict_states = {}
    dict_conns = {}

    proc_id = 0
    while list_states:
        if proc_id not in dict_states: dict_states[proc_id] = []
        dict_states[proc_id].append(list_states.pop())
        proc_id += 1
        proc_id = proc_id % number_of_processes

    for id in dict_states:
        print id, dict_states[id]


    # Run fresh state-wide disambiguation batches. Alternatively, we can skip this and read from pickled files from previous runs.
    for id in dict_states:
        # queue = multiprocessing.Queue()
        conn_parent, conn_child = multiprocessing.Pipe()
        dict_conns[id] = (conn_parent, conn_child)

        p = multiprocessing.Process(target=worker_disambiguate_states, name=str(id), args=(conn_child,))

        list_jobs.append(p)
        time.sleep(1)
        p.start()
        conn_parent.send(dict_states[id])

    for p in list_jobs:
        p.join()
    return




def combine_affiliation_graphs():
    '''
    Combine the affiliation graphs of states into a national one.
    Recompute the edge significances.
    '''
    __combine_affiliation_graphs_occupation()
    __combine_affiliation_graphs_employer()





def __combine_affiliation_graphs_occupation():
    '''
    Combine the occupation graphs of all states into one national one.
    '''
    # load all graphs
    list_G = []
    for abbr, state in utils.states.dict_state.iteritems():
        print state
        filename = config.affiliation_occupation_file_template % state
        try:
            list_G.append((state, utils.igraph.Graph.Read_GML(filename)))
        except:
            pass

    edgelist = {}
    for state, G in list_G:
        for e in G.es:
            v1 = G.vs[e.target]
            v2 = G.vs[e.source]
            l1 = v1['label']
            l2 = v2['label']
            labels = tuple(sorted([l1, l2]))
            try:
                weight = e['weight']
            except:
                continue
            try:
                edgelist[labels] += weight
            except:
                edgelist[labels] = weight
    edgelist = [(x[0][0], x[0][1], x[1]) for x in edgelist.items()]
    G = utils.igraph.Graph.TupleList(edgelist, edge_attrs='weight', vertex_name_attr="label")
    filename = config.affiliation_occupation_file_template % "USA"

    # conmpute significances
    utils.filters.compute_significance(G)

    G.write_gml(filename)





def __combine_affiliation_graphs_employer():
    '''
    Combine the employer graphs of all states into one national one.
    '''
    # load all graphs
    list_G = []
    for abbr, state in utils.states.dict_state.iteritems():
        print state
        filename = config.affiliation_employer_file_template % state
        try:
            list_G.append((state, utils.igraph.Graph.Read_GML(filename)))
        except:
            pass

    edgelist = {}
    for state, G in list_G:
        for e in G.es:
            v1 = G.vs[e.target]
            v2 = G.vs[e.source]
            l1 = v1['label']
            l2 = v2['label']
            labels = tuple(sorted([l1, l2]))
            try:
                weight = e['weight']
            except:
                continue
            try:
                edgelist[labels] += weight
            except:
                edgelist[labels] = weight
    edgelist = [(x[0][0], x[0][1], x[1]) for x in edgelist.items()]
    G = utils.igraph.Graph.TupleList(edgelist, edge_attrs='weight', vertex_name_attr="label")
    filename = config.affiliation_employer_file_template % "USA"

    # conmpute significances
    utils.filters.compute_significance(G)

    G.write_gml(filename)






import sys
if __name__ == "__main__":

    disambiguate_multiple_states()
    sys.exit()
    # print "AFFILATION: zip_code\n" + "_"*80 + "\n"*5
    # generateMigrationData('delaware', affiliation='zip_code', record_limit=(0, 5000000), num_procs = 12)
    # quit()


#   print "AFFILATION: OCCUPATION\n" + "_"*80 + "\n"*5
#     generateAffiliationData('delaware', affiliation=None, record_limit=(0, 500000), num_procs=1)
#     quit()
#
#     print "AFFILATION: EMPLOYER\n" + "_"*80 + "\n"*5
#     generateAffiliationData('delaware', affiliation='employer', record_limit=(0, 500))

    # Generate networks for both employers and occupations
#     generateAffiliationData('massachusetts', affiliation=None, record_limit=(0, 5000000))
#     quit()

    # init.INIT_combine_state_tokens_and_vectors()
    # quit()

#     tokenize_all_states_uniform()
#     quit()



    print "DISAMBIGUATING    \n" + "_"*80 + "\n"*5
    project = disambiguate_main('newyork',
                       record_limit=(0, 500000),
                       logstats=False,
                       # whereclause=" WHERE NAME LIKE '%COHEN%' ",
                       # whereclause=" WHERE NAME like '%AARONS%' ",
                       # whereclause=" WHERE NAME like '%COHEN%' ",
                       num_procs=12,
                       percent_employers=50,
                       percent_occupations=50)
    quit()

    project.D.save_identities_to_db()
    # process_last_names(project)
    # process_middle_names(project)


    quit()


    hand_code('newyork', record_limit=(0, 10000), sample_size=10000, logstats=True)
    quit()


    quit()

    pass


