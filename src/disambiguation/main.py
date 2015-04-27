#! /usr/bin/python
# This program analyzes the full database for a given state. That is, not just the <state>_addresses
# 
# The <state>_addresses tables are roughly 0.25 the size of their corresponding <state> tables. Why?
# My explanation is that lots of records that have unique identifier pairs in the <state> table, 
# occur multiple times in the contributor_addresses table, and therefore don't appear in its unique
# version, namely contributor_addresses_unique.
import init
''' TODO:
    1- (DONE: <state>_combined) We need a new table that combines <state> and <state>_addresses as follows:
        The table structure is like <state>_addresses, but it contains all the records found in <state>.
        For records that have address fields in <state>_addresses, use those. For others, use null.

    2- This table will be analyzed in this code.

    3- Add additional fields to each record:
        Name frequency, Timeline period (if exists), Size of neighborhood in employer graph?, Centrality in Employer graph? Size of neighborhood in occupation graph

    4- Identify the right set of features of the records from this table to use with Disambiguator.
        Candidates: Name, Address, Employer, Occupation, CMTE_ID (may be confounding)

    5- Define "possible match". Identifying possible matches will be the first stage of disambiguation. We will refine our knowledge starting from these "possible matches".
        ? Name is similar (specify)
        ? Address is same
        ? Employer is
        '''
'''
Strategy:
    - Using <state>_addresses, find possible matches (use Disambiguator)
    - Using the results, use AffiliationAnalyzer to derive network of affiliations
    -
    - Query <state>_combined. Contains all records from <state>, and for some, additional data from <state>_addresses
    - Find possible matches using Disambiguator and additional attributes: frequencies, affiliations, etc.
    - A different comparison function is needed.
    -
'''
''' Notes:
    1- The <state> tables are 3~4 times as large as <state>_addresses tables.
    2- The <state>_address and <state>_combined tables don't have all the FIRST_NAMEs and LAST_NAMEs so
        if I use these tables to get names, some records won't have names.
'''
# GLOBALS
# String template for file containing token data for each
# state. Example:
# project["data_path"] + param_state + "-tokendata.pickle"

from collections import OrderedDict

from core import Project
from core.Affiliations import AffiliationAnalyzerUndirected, MigrationAnalyzerUndirected
from core.Database import FecRetriever
import core.Disambiguator as Disambiguator
from core.Tokenizer import Tokenizer, TokenizerNgram, TokenData
import config
from core.states import *
from core.utils import *
import resource


def find_all_in_list(regex, str_list):
    ''' Given a list of strings, str_list and a regular expression, regex, return a dictionary with the
        frequencies of all mathces in the list.'''
    dict_matches = {}
    for s in str_list:
#         s_list = re.findall(r'\b\w\b', s)
        s_list = re.findall(regex, s)
        for s1 in s_list:
            if s1 in dict_matches:
                dict_matches[s1] += 1 
            else:
                dict_matches[s1] = 1
    return dict_matches

 


def loadAffiliationNetwork(label, data_path, affiliation, percent=5):
    '''
    Loads the saved output of AffiliatoinAnalyzer from file: the affiliation network.
    It also adds a new attribute to the graph instance that contains a dictionary from
    affiliation identifier strings to the index of their corresponding vertex in the graph object.

    TODO: allow filtering based on value of an edge (or vertex) parameter
    '''
            
    def prune(G, field='significance', percent=5):
        '''
        Remove all but the top X percent of the edges with respect to the value of their field.
        '''
        deathrow = []
        n = len(G.es)
        threshold_index = n - n * percent / 100
        threshold_value = sorted(G.es[field])[threshold_index]
        
        for e in G.es:
            if e[field] < threshold_value: 
                deathrow.append(e.index)
        G.delete_edges(deathrow)
        return G
    
    try:
        filename = f = data_path + label + affiliation + '_graph.gml'
        print filename
        G = igraph.Graph.Read_GML(filename)
        
        G = prune(G, field='significance', percent=percent)
        
        dict_string_2_ind = {v['label']:v.index for v in G.vs}
        G.dict_string_2_ind = dict_string_2_ind
    except IOError:
        print "ERROR: Affiliation Network data not found."
        G = None
    
    # Not really necessary any more. I construct a {string: index} dictionary from the loaded Graph myself. 
    # metadata = json.load(open(data_path + label + '-' + affiliation + '-metadata.json'))
    return G




    
def tokenize_all_states_uniform(record_limit=(0, 20000000)):
    '''
    BASICALLY USELESS, because tokenizing all states together comsumes
    way too much memory and it's multiprocessing's fauls I think.
    Worker function that can compute the tokens and coarse feature vectors
    for a given state. Can be used as a child process worker by a parent
    process for parallel processing.
    '''
    batch_id = get_next_batch_id()
    project = Project.Project(batch_id=batch_id)
    
    project.putData('state' , 'USA')


    
    record_start, record_no = record_limit   

    project.putData('batch_id' , batch_id)
    
    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    project.putData("list_tokenized_fields", list_tokenized_fields)
    
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)
    
    all_fields = list_tokenized_fields + list_auxiliary_fields 
    project.putData('all_fields' , all_fields)
    
    # dictionaries indicating the index numbers associated with all fields
    index_2_field = { all_fields.index(s):s for s in all_fields}
    project.putData("index_2_field", index_2_field)
    
    field_2_index = { s:all_fields.index(s) for s in all_fields}
    project.putData("field_2_index", field_2_index)
    
    whereclause = " WHERE ENTITY_TP='IND' "
    
    list_of_records = []
    
    
    for param_state in get_states_sorted()[:1]:
        table_name = param_state + "_combined"
        retriever = FecRetriever(table_name=table_name,
                          query_fields=all_fields,
                          limit=(record_start, record_no),
                          list_order_by=['NAME', "TRANSACTION_DT", "ZIP_CODE", "CMTE_ID"],
                          where_clause=whereclause)
        retriever.retrieve()
    #     project.putData("query", retriever.getQuery())
        print "number of records for state ", param_state, ": ", len(retriever.list_of_records)
        
        list_of_records += retriever.getRecords()
        retriever = None
        
    # Make sure tokens and feature vectors are generated and save to file.    
    TokenizerClass = Tokenizer
    tokenizer = TokenizerClass()
    project.tokenizer = tokenizer
    tokenizer.project = project
    tokenizer.setRecords(list_of_records)
    tokenizer.setTokenizedFields(project['list_tokenized_fields'])
    
        
    print "Tokenizing records..."
    tokenizer.tokenize(num_procs=12)
    quit()
    




def generateAffiliationData(state=None, affiliation=None, record_limit=(0, 5000000), whereclause='', num_procs=1):
    '''
    One version of main().
    Operates on <state>_addresses tables in order to find
    high-certainty identities so that affiliation networks
    can be generated. The output of this method will be the
    data files which can be loaded by loadAffiliationNetwork().
    The comparison method used by Records here should be
    strict_address.
    '''
    
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

            

   



def disambiguate_main(state, record_limit=(0, 5000000), method_id="thorough", logstats=False, whereclause='', num_procs=1, percent_employers=5, percent_occupations=5):  
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



    Parameters:
        percent_employers: percentage of top edges in the employers network to use.
        percent_occupations: percentage of top edges in the occupations network to use.

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
        quit()


    
#     # A list(1) of lists(2)  where each list(2) corresponds to one of the records returned by MySQL
#     # and it contains only the "identifier" fields of that record. This is the main piece of data processed by this program.
#     list_of_records_identifier = [[record[index] for index in index_list_tokenized_fields] for record in tmp_list]
#     
#     # Same as above, except each list(2) consists of the auxiliary columns of the record returned by MySQL.
#     list_of_records_auxiliary = [[record[index] for index in index_auxiliary_fields] for record in tmp_list]  
    
    
    
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
    
    G_employer = loadAffiliationNetwork(param_state + "-", config.dict_paths["data_path_affiliations_employer"], 'employer', percent=percent_employers)
    if G_employer:
        for record in list_of_records:
            record.list_G_employer = [G_employer]
    else:
        print "WARNING: EMPLOYER network not found."
        project.log('WARNING', param_state + " EMPLOYER network not found.")

    
    
    G_occupation = loadAffiliationNetwork(param_state + "-" , config.dict_paths["data_path_affiliations_occupation"], 'occupation', percent=percent_occupations)
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
    
    G_employer = loadAffiliationNetwork(param_state, config.dict_paths["data_path_affiliations_employer"], 'employer')
    if G_employer:
        for record in list_of_records:
            record.list_G_employer = [G_employer]
        print "G_employer loaded"
    else:
        print "WARNING: EMPLOYER network not found."
        project.log('WARNING', param_state + " EMPLOYER network not found.")

    
    
    G_occupation = loadAffiliationNetwork(param_state, config.dict_paths["data_path_affiliations_occupation"], 'occupation')
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
        
    quit()
    
    
    
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






if __name__ == "__main__":

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
    
    #init.INIT_combine_state_tokens_and_vectors()
    #quit()
    
#     tokenize_all_states_uniform()    
#     quit()

    print "DISAMBIGUATING    \n" + "_"*80 + "\n"*5
    project = disambiguate_main('delaware',
                       record_limit=(0, 5000000),
                       
                       logstats=False,
                       # whereclause=" WHERE NAME LIKE '%COHEN%' ",
                       # whereclause=" WHERE NAME like '%AARONS%' ",
                       # whereclause=" WHERE NAME like '%COHEN%' ",
                       num_procs=1,
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

 
