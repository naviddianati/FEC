'''
This module contains methods that prepare data and export intermediate computation results to files.
If all methods in this module are run once successfully, the following data files will exist in the
relevant paths and will be available to disambiguation methods:

- tokendata files for each state, both using Tokenizer and TokenizerNgram tokenization schemes.
- feature vector files for each state both using Tokenizer and TokenizerNgram tokenization schemes.
- LHS hash files for each state both using Tokenizer and TokenizerNgram tokenization schemes.

- combined (national, "USA") tokendata file derived from Tokenizer (coarse) results.
- combined (national, "USA") feature vector file derived from Tokenizer (coarse) results.
- combined (national, "USA") hash file derived from Tokenizer (coarse) results.

1. For fine-grained intra-state disambiguation:
    - Tokenize and compute vectors for every state using TokenizerNgram. 
    - Compute hashes from the vectors computed above.
2. For coarse-grained nationwide disambiguation:  
    - Tokenize and compute vectors for every state using Tokenizer. 
    - Combine Tokenizer tokendata into the "USA" files and update the vectors.
    - Compute coarse national hashes from the vectors computed above.        
'''

from core.utils import *
import core.Project as Project
import disambiguation.config as config
from core.Database import FecRetriever
import core.Disambiguator as Disambiguator
from core.Tokenizer import Tokenizer, TokenizerNgram, TokenData
from core.states import *
    
    
    

def tokenize_records(list_of_records, project, TokenizerClass):
    '''
    Return a TokenDadta object for list_of_records, and update each
    record in list_of_records by adding to it a vector attribute and
    associating the TokenData instance to the record. If a file
    containing the pickled TokenData instance already exists, load it.
    Also, make sure that the vectors are available in a file for later
    use. If either file doesn't exist, start a new TokenizerClass instance
    and compute and export both. At the end of this function, both the
    tokendata and the vectors will exist in pickled format in files.

    @param TokenizerClass: The Tokenizer class used. It can be
        TokenizerNgram, Tokenizer, etc.
    '''
    tokendata_file = config.tokendata_file_template % (project['state'], TokenizerClass.__name__)
    vectors_file = config.vectors_file_template % (project['state'], TokenizerClass.__name__)
    
    
    try:
        tokenizer = TokenizerClass()
        print 1
        project.tokenizer = tokenizer
        print 2
        tokenizer.project = project
        print 3
        tokenizer.setRecords(list_of_records)
        print 4
        tokenizer.setTokenizedFields(project['list_tokenized_fields'])
        print 5
        
        tokenizer.load_from_file()
        print 6        
        # Just make sure the vectors are also there.
        if os.stat(vectors_file).st_size == 0:
            raise Exception("vectors file not found.")
        list_of_records = tokenizer.getRecords()
    
    except Exception as e:
        print "error occurred"
        print str(e)
                
        tokenizer = TokenizerClass()
        project.tokenizer = tokenizer
        tokenizer.project = project
        tokenizer.setRecords(list_of_records)
        tokenizer.setTokenizedFields(project['list_tokenized_fields'])
        
        
        print "Tokenizing records..."
        tokenizer.tokenize()
        
        print "Saving token data to file..."
        tokenizer.tokens.save_to_file(tokendata_file)
        list_of_records = tokenizer.getRecords()

    return tokenizer, list_of_records
        




def INIT_compute_national_hashes(num_procs=1):
    '''
    From the combined national vector and tokendata files, compute
    LSH hashes and export to "USA" hash file. Use coarse feature 
    vectors for this purpose. That is, take words as tokens not 
    unigrams and bigrams. The goal is to be able to quickly and 
    easily detect identical names, employers, etc. across different states.
    '''
    project = Project.Project(1)
    project.putData('state' , 'USA')
    
    filelabel = "USA"
    file_tokens = config.tokendata_file_template % (filelabel, 'Tokenizer')
    f = open(file_tokens)
    tokendata = cPickle.load(f)
    f.close()
    
    # dimension of input vectors
    dim = tokendata.no_of_tokens

    D = Disambiguator.Disambiguator([], dim, matching_mode="strict_address", num_procs=num_procs)
    project.D = D
    D.project = project
    D.tokenizer = Tokenizer()
    

    # desired dimension (length) of hashes
    hash_dim = 60
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 40
        
    print 'Generating the LSH hashes'
    
    # compute the hashes
    D.compute_hashes(hash_dim, num_procs)
    
    print "Done computing hashes."

  


def INIT_combine_state_tokens_and_vectors():
    '''
    Read all Tokenizer tokendata objects for different states and combine them.
    Then read every state feature vector file and update it according
    to the compound tokendata, and write all of them to a file.

    Here we need the data (tokendata, vectors, etc) generated using the coarse
    tokenizer class Tokenizer. This is different from the data used for fine-
    grained analysis withing each state. That data is based on the TokenizerNgram
    tokenizer class.
    '''
    def update_vectors(dict_vectors, compoundtokendata, old_tokendata):
        '''
        for every r_id: r_vector pair in dict_vectors, update translate the vector
        from old_tokendata to compoundtokendata.
        '''
        compound_dict_vectors = {}
        for r_id, old_vector in dict_vectors.iteritems():
            vector = {}
            # translate self.vector
            for index_old in old_vector:
                index_new = compoundtokendata.token_2_index[old_tokendata.index_2_token[index_old]]
                vector[index_new] = 1
            compound_dict_vectors[r_id] = vector
        return compound_dict_vectors
    
    filelabel = "USA"
    
    # list of tokendata objects
    list_tokendata = []
    print " combining tokendata objects..."
    for param_state in get_states_sorted():
        print "processing state: ", param_state
        file_tokens = config.tokendata_file_template % (param_state, 'Tokenizer')
        f = open(file_tokens)
        list_tokendata.append(cPickle.load(f))
    compoundtokendata = TokenData.getCompoundTokenData(list_tokendata)
    del list_tokendata[:]
    print "Done combining state tokendata objects."
    
    
    # Save compound tokendata to file
    file_tokens = config.tokendata_file_template % (filelabel, 'Tokenizer')
    f = open(file_tokens, 'w')
    cPickle.dump(compoundtokendata, f)
    f.close()
    
    
    
    # Load feature vector files for all states one at a time and
    # update the vectors.
    dict_vectors_all = {}
    print "Updating feature vectors..."
    for param_state in get_states_sorted():
        print "processing state: ", param_state
        file_vectors = config.vectors_file_template % (param_state, 'Tokenizer')
        f = open(file_vectors)
        dict_vectors = cPickle.load(f)
        f.close()
        
        file_tokens = config.tokendata_file_template % (param_state, 'Tokenizer')
        f = open(file_tokens)
        old_tokendata = cPickle.load(f)
        f.close()
        
        dict_vectors = update_vectors(dict_vectors, compoundtokendata, old_tokendata)
        dict_vectors_all.update(dict_vectors)
    print "Done updating feature vectors."
        
    # Export computed vectors to file
    vectors_file = config.vectors_file_template % (filelabel, 'Tokenizer')

    try:
        f = open(vectors_file, 'w')
        cPickle.dump(dict_vectors_all, f)
        f.close()
    except:
        os.remove(vectors_file)
        raise



def INIT_process_single_state(state=None, TokenizerClass=None,  list_tokenized_fields = [], record_limit=(0, 50000000), whereclause='', num_procs=1):
    '''
    Using the TokenizerClass specified, tokenize all records from the specified 
    state, save the tokendata as well as the feature vectors to files. 
    Then, compute the LSH hashes and save to file.
    
    '''
    
    batch_id = 1
    project = Project.Project(batch_id)
    project.putData('batch_id' , batch_id)

    
    if state is None: 
        raise Exception("Error: you must specify the state to tokenize.")
    param_state = state
        
    print "Tokenizing state %s using TokenizerClass: %s ", (param_state, TokenizerClass.__name__) 
    
    table_name = param_state + "_combined"
  
    project.putData('state' , param_state)
    
    record_start = record_limit[0]
    record_no = record_limit[1]
    
    if not list_tokenized_fields:
        list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']

    project.putData("list_tokenized_fields", list_tokenized_fields)
    
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    project.putData("list_auxiliary_fields", list_auxiliary_fields)
    
    all_fields = list_tokenized_fields + list_auxiliary_fields 
    project.putData('all_fields' , all_fields)
    
    # dictionaries indicating the index numbers associated with all fields
#     index_2_field = { all_fields.index(s):s for s in all_fields}
#     project.putData("index_2_field", index_2_field)
#     
#     field_2_index = { s:all_fields.index(s) for s in all_fields}
#     project.putData("field_2_index", field_2_index)
    
    
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
    
    # Tokenize, and save tokendata and vectors to file.
    tokenizer, list_of_records = tokenize_records(list_of_records, project, TokenizerClass)
    tokendata = tokenizer.tokens
    
   
    # dimension of input vectors
    dim = tokendata.no_of_tokens

    D = Disambiguator.Disambiguator(list_of_records, dim, matching_mode="strict_address", num_procs=num_procs)
    project.D = D
    D.project = project
    D.tokenizer = tokenizer
    
    # desired dimension (length) of hashes
    hash_dim = 60
    project.putData('hash_dim' , str(hash_dim))

    # In D, how many neighbors to examine?
    B = 40
    
    # Number of times the hashes are permutated and sorted
    no_of_permutations = 20
    project.putData('number_of_permutations' , str(no_of_permutations))
    
    # compute the hashes
    print "Computing LSH hashes for state %s using TokenizerClass: %s" % (param_state, TokenizerClass.__name__) 
    D.compute_hashes(hash_dim, num_procs)
    print "Done computing LSH hashes."



def worker_process_multiple_states(conn):
    '''
    Worker method run by each child process spawned by INIT_process_multiple_states.
    '''
    data = conn.recv()
    proc_name = multiprocessing.current_process().name
    
    print proc_name, data['list_states']
    
    
    for state in data['list_states']:
        try:
            INIT_process_single_state(state, data['TokenizerClass'],
                                       list_tokenized_fields = data['list_tokenized_fields'],
                                       num_procs = 1)
        except Exception as e:
            print "Failed to run INIT_process_single_state for state ", state
            print "Error ", e


 

def INIT_process_multiple_states(list_states = [], TokenizerClass=None, list_tokenized_fields = [], num_procs = 12):
    '''
    Using multiple processes, perform INIT_process_single_state for multiple
    states at the same time. 
    
    @param list_states: list of states. If empty, all states are processed.
    @param TokenizerClass: the tokenizer class to be used. For fine-grained
        intra state disambiguation, use TokenizerNgram. For coarse national
        disambiguation use Tokenizer.
    @parm num_procs: total number of processes we can use in different stages. 
    '''


    list_jobs = []

    # if not specified,  load all states
    if not list_states:
        list_states = states.get_states_sorted()

    list_states.reverse()
    
    # Number of states to be processed.
    N = len(list_states)

    # No more than num_procs processes
    number_of_processes = min(N, num_procs)
    
    dict_states = {}
    
    # dictionary of connections to child processes.
    dict_conns = {}


    proc_id = 0
    while list_states:
        if proc_id not in dict_states: dict_states[proc_id] = set()
        dict_states[proc_id].add(list_states.pop())
        proc_id += 1
        proc_id = proc_id % number_of_processes

    for id in dict_states:
        print id, dict_states[id]


    # Run fresh state-wide disambiguation batches. 
    for id in dict_states:
        # queue = multiprocessing.Queue()
        conn_parent, conn_child = multiprocessing.Pipe()
        dict_conns[id] = (conn_parent, conn_child)        

        p = multiprocessing.Process(target=worker_process_multiple_states, name=str(id), args=(conn_child,))

        list_jobs.append(p)
        time.sleep(1)
        p.start()
        data = {'list_states': dict_states[id],
                'TokenizerClass': TokenizerClass,
                'list_tokenized_fields': list_tokenized_fields }
        conn_parent.send(data)

    for p in list_jobs:
        p.join()
    




if __name__ == "__main__":
    
    
    '''State level data preparation (for fine-grained intra state disambiguation)'''
    # Tokenize, vectorize and hashify all states using TokenizerNgram
    #INIT_process_multiple_states(TokenizerClass = TokenizerNgram, num_procs = 12)
    
    ''' National level data preparation: '''
    # Tokenize, vectorize and hashify all states using Tokenizer
    #INIT_process_multiple_states(TokenizerClass = Tokenizer, num_procs = 12)

    
    # combine the vectors and tokens from all states into the national data files.
    #INIT_combine_state_tokens_and_vectors()
    
    # Using the national vectors and tokens, compute uniform national hashes
    INIT_compute_national_hashes(num_procs=10)
    
    
    
    
    
