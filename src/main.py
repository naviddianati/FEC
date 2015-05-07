#! /usr/bin/python
# This program analyzes the full database for a given state. That is, not just the <state>_addresses
#
# The <state>_addresses tables are roughly 0.25 the size of their corresponding <state> tables. Why?
# My explanation is that lots of records that have unique identifier pairs in the <state> table,
# occur multiple times in the contributor_addresses table, and therefore don't appear in its unique
# version, namely contributor_addresses_unique.
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


import disambiguation.init as init

from collections import OrderedDict

from disambiguation.core import Project
from disambiguation.core.Affiliations import AffiliationAnalyzerUndirected, MigrationAnalyzerUndirected
from disambiguation.core.Database import FecRetriever
import disambiguation.core.Disambiguator as Disambiguator
from disambiguation.core.Tokenizer import Tokenizer, TokenizerNgram, TokenData
import disambiguation.config as config
from disambiguation.core.states import *
from disambiguation.core.utils import *











def DISAMBIGUATE_stage_1():
    '''
    Run disambiguate_main for each state. The main products will be the following:

    - The preliminary identities inferred at the state level, stored in the
        "identities" MySQL table.
    - The full match buffers of all states. These are lists of all record pairs
        matched within each state.
    - The near misses within each state. These can be used to revisit and refine
        the clusters found, in the second round.
    '''
    import stage1

    # Disambiguate every state separately in parallel.
    stage1.disambiguate_multiple_states(list_states=[], num_procs=12)




def DISAMBIGUATE_stage_2():
    '''
    Using results from state-level disambiguation, do a second run of comparisons
    on a national level. The goal is to match clusters across states and even within
    states. To do this, we use the following data:
    - coarse hashes computed for the entire country using the Tokenizer class via
        INIT_compute_national_hashes()
    - The match buffer of each state, used to avoid double-checking records that are
        already matched. That would be stupid.
    - The set of near miss record pairs found for each state. These pairs will be re-
        examined if they aren't clustered together already. The point is that using the
        stage_1 identities, we now have access to new statistics that will let us
        assess the likelihood of these near-misses actually being matches.

    The steps are roughly as follows:
    - Using the uniform national hashes, find a list of similar record pairs across
        the country. This list minus the match buffers from stage 1 will be the pairs
        we will be comparing in this round.
    - Divide records into num_procs "independent" sets of roughly equal sizes. That is,
        divide into sets such that all pair comparisons we need to do fall within the
        sets. Each one of these sets will be sent to a child process for processing.
    '''
    import stage2
    # Number of new record pairs to compare at the national level
    num_pairs = 10000

    # Number of processes to use for stage 2 disambiguation.
    num_procs = 12

    # Get pairs of record ids that are similar according
    # to the national (combined) hashes, but aren't already
    # linked at the state level.
    list_record_pairs = stage2.get_candidate_pairs(num_pairs)


    # Partition the full record set into num_procs subsets
    # with minimal inter-set links, and export the record ids
    # to a separate file for each subset.
    list_filenames = stage2.partition_records(list_record_pairs, num_procs, file_label="")


    # Compare record pairs within each subset and save results.
    stage2.disambiguate_subsets_multiproc(list_filenames, num_procs)

    pass







from disambiguation.core.hashes import get_edgelist_from_hashes_file
import igraph as ig
def test_hashes():
    import stage2
    state = 'USA'

    # Load normalized attributes
    filename = config.normalized_attributes_file_template % state
    f = open(filename)
    dict_normalized_attributes = cPickle.load(f)
    f.close()

#     return
#     for x in  dict_normalized_attributes:
#         print x
#     return
    avg_degree = 1



    num_hashes = len(dict_normalized_attributes)


    edgelist = stage2.get_candidate_pairs(100000, 'USA')
    # sort edgelist
    # COMES SORTED
    # edgelist.sort(key=lambda x: x[2], reverse=True)
    f = open('edgelist.txt', 'w')
    for edge in edgelist:
        f.write('%d %d %f\n' % edge)
        d0 = dict_normalized_attributes[edge[0]]
        d1 = dict_normalized_attributes[edge[1]]
        print edge[2], "=" * 70
        print edge[0], d0['N_first_name'], d0['N_middle_name'], d0['N_last_name'], d0['N_zipcode'], d0['N_occupation'], d0['N_employer'], d0['N_address']
        print edge[1], d1['N_first_name'], d1['N_middle_name'], d1['N_last_name'], d1['N_zipcode'], d1['N_occupation'], d1['N_employer'], d1['N_address']
    f.close()
    return

    ecount = int(avg_degree * num_hashes)
    edgelist = edgelist[:ecount]
    g = ig.Graph.TupleList(edgelist, edge_attrs='score')
    g.write_gml(config.data_path + 'delaware_edlist.gml')
    list_components = g.components().subgraphs()

    set_names = set([int(v['name']) for v in g.vs])


    print "number of components: ", len(list_components)
    for component in  sorted(list_components, key=lambda g : dict_normalized_attributes[int(g.vs[0]['name'])]['N_last_name']):
        for v in component.vs:
            d = dict_normalized_attributes[int(v['name'])]
            d['id'] = int(v['name'])
            print d['id'], d['N_first_name'], d['N_middle_name'], d['N_last_name'], d['N_zipcode'], d['N_occupation'], d['N_employer'], d['N_address']
        for e in component.es:
            v0 = component.vs[e.target]
            v1 = component.vs[e.source]
            print "(%s , %s) -- %f" % (v0['name'], v1['name'], e['score'])
        print "=" * 70




def view_vectors(state='delaware'):
    '''
    For a sample of records in state, print the normalized attributes
    and feature vector.
    '''

    dict_vectors = load_feature_vectors(state, 'Tokenizer')
    tokendata = load_tokendata(state, 'Tokenizer')
    dict_norm_attr = load_normalized_attributes(state)

    counter = 0
    for r_id, norm_attr in dict_norm_attr.iteritems():
        print [tokendata.index_2_token[token_id] for token_id in dict_vectors[r_id]]
        print [norm_attr[key] for key in sorted(norm_attr.keys())]
        print "=" * 70
        if counter > 10: break
        counter += 1



def INIT():

    '''State level data preparation (for fine-grained intra state disambiguation)'''
    # Tokenize, vectorize and hashify all states using TokenizerNgram
    # init.INIT_process_multiple_states(TokenizerClass = TokenizerNgram, num_procs = 12)

    ''' National level data preparation: '''
    # Tokenize, vectorize and hashify all states using Tokenizer
    #init.INIT_process_multiple_states(TokenizerClass=Tokenizer, num_procs=12)


    # combine the vectors and tokens from all states into the national data files.
    # init.INIT_combine_state_tokens_and_vectors()

    # Using the national vectors and tokens, compute uniform national hashes
    init.INIT_compute_national_hashes(num_procs=10)


    
def test_memory1():
    '''
    load the USA 20-char hashes to see memory 
    use.
    '''
    import disambiguation.core.utils as utils 
    utils.time.sleep(10)
    print "loading hashes"
    filename = config.hashes_file_template % ('USA', 'Tokenizer')
    with open(filename) as f:
        dict_hashes = cPickle.load(f)
    print "now converting."
    ids = []
    ss = []
    for rid in dict_hashes.keys():
        s = dict_hashes.pop(rid)
        ids.append(rid)
        ss.append(s)
    del dict_hashes
    print "done"
    utils.time.sleep(60)
    quit()



def test_identity_manager():
    from disambiguation.core.Database import IdentityManager
    from disambiguation.core import utils
    
    idm = IdentityManager(state='USA')
    idm.fetch_dict_id_2_identity()
    print "Done"
    utils.time.sleep(60)
    return
    for identity, rids in idm.dict_identity_2_list_ids.iteritems():
        print identity, rids


if __name__ == "__main__":

#     filename = config.hashes_file_template % ('delaware','Tokenizer')
#     edgelist = get_edgelist_from_hashes_file(filename, 20, 5, num_procs = 3)
#     print len(edgelist)
#     quit()

#     view_vectors()
#     quit()

    import stage2
    list_pairs = stage2.get_candidate_pairs(1000000, 'USA')
    for edge in list_pairs:
        print edge[0], edge[1], edge[2]
    quit()


    test_hashes()
    quit()




    test_identity_manager()
    quit()










    INIT()
    quit()









    states.get_states_sorted('num_records')
    quit()



    test_identity_manager()
    quit()



