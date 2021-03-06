#! /usr/bin/python






import disambiguation.init as init

from collections import OrderedDict

from disambiguation.core import Project, search
from disambiguation.core.Affiliations import AffiliationAnalyzerUndirected, MigrationAnalyzerUndirected
from disambiguation.core.Database import FecRetriever, FecRetrieverByID
import disambiguation.core.Disambiguator as Disambiguator
from disambiguation.core.Tokenizer import Tokenizer, TokenizerNgram, TokenizerName, TokenData
import disambiguation.config as config
from disambiguation.core.states import *
from disambiguation.core.utils import *











def DISAMBIGUATE_stage_1():
    '''
    Run L{disambiguate_main} for each state. The main products will be the following:

        - The preliminary identities inferred at the state level, stored in the
        "identities" MySQL table.
        - The full match buffers of all states. These are lists of all record pairs
        matched within each state.
        - The near misses within each state. These can be used to revisit and refine
        the clusters found, in the second round.
    '''
    import stage1

    # TODO: compute pre-stage1 affiliations
    # Now it's in a separate script, not integrated into
    # the unified pipeline.

    # Combine affiliation graphs into a national one.
    stage1.combine_affiliation_graphs()


    # Create the identities and identities_adjacency tables
    # needed for disambiguation.
    stage1.create_tables(overwrite=True)


    # Disambiguate every state separately in parallel.
    # list_done_abbr = ['VA', 'MN' ,'PA' ,'CO' ,'IL' ,'AZ' ,'NJ' ,'SD' ,'NC' ,'OH' ,'OR' ,'NH' ,'OK' ,'NE' ,'MA' ,'AL' ,'KY' ,'DE' ,'VT' ,'HI' ,'AS' ,'FM']
    # already_done = [dict_state[abbr] for abbr in list_done_abbr]
    # print already_done


    list_states = get_state_order().items()
    list_states = [x for x, y in sorted(list_states, key=lambda x:x[1])]
    # already_done = ['ohio','oklahoma','arizona','minnesota','colorado','connecticut','newjersey','massachusetts','pennsylvania','virginia','illinois']
    # for state in already_done: list_states.remove(state)
    # list_states = ['california', 'florida','newyork']
    # list_states = []
    print list_states
    stage1.disambiguate_multiple_states(list_states=list_states, num_procs=10)

    # After stage1 disambiguation, use the identities to generate
    # a second iteration of the affiliation graphs.
    stage1.generateAffiliationDataPostStage1_multiple_states(list_states=list_states, num_procs=3)

    # Combine post-stage1 affiliation graphs into a national one.
    stage1.combine_affiliation_graphs(poststage1=True)



def DISAMBIGUATE_stage_2():
    '''
    Using results from state-level disambiguation, do a second run of comparisons
    on a national level. The goal is to match clusters across states and even within
    states. To do this, we use the following data:
        - coarse hashes computed for the entire country using the Tokenizer class via
        L{INIT_compute_national_hashes}
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
        - Figure out which "identities" these similar records belong to and make a list of
        such pairs of identities that will need to be compared.
        - Divide these identity pairs into num_procs "independent" sets of roughly equal sizes.
        For each set, load all the records belonging to the identities it contains.
        Each one of these sets will be sent to a child process for processing.
    '''
    import stage2
    # Number of new identity pairs to compare at the national level
    num_pairs = 100000000

    idm = Database.IdentityManager('USA')

    # Number of processes to use for stage 2 disambiguation.
    num_procs = 12

    # Compute token frequencies at the person level given the
    # identities computed in stage1
    stage2.compute_person_tokens()

    # Get pairs of record ids that are similar according
    # to the national (combined) hashes, but aren't already
    # linked at the state level.
    stage2.get_candidate_pairs(num_pairs, recompute=True, mode='disambiguation')

    # Partition the set of all S1 identities containing any of the
    # candidate records into subsets with no overlaps. Export the
    # lists of identity pairs to a separate file for each partition.
    # Also, exoirt the list of all record ids associated with any
    # of the identities in each partition into a separate file for
    # that partition as well.
    stage2.partition_S1_identities(num_partitions=200, state='USA', idm=idm, recompute_identity_partitions=False)

    # Compare record pairs within each subset and save results.
    stage2.disambiguate_subsets_multiproc(num_partitions=200, state="USA", num_procs=10, idm=idm)








def INIT():
    '''
    Initialize. Perform preliminary computations in preparation for the stage1
    and stage2 disambiguations. We compute a number of things and export the resulting
    data to files. These files will be loaded later by disambiguation methods.
        - For each state, using the TokenizerNgram class, tokenize all records from the specified
        state, save the tokendata as well as the feature vectors to files. Then, compute the LSH hashes
        and save to file. This data will be used for the state-level (stage1) disambiguation where
        candidate record pairs are selected based on hashes computed from file-grained (Ngram)
        feature vectors.
        -  For each state, using the Tokenizer class, tokenize all records from the specified
        state, save the tokendata as well as the feature vectors to files. Then, compute the LSH hashes
        and save to file. This data will be used for the national (stage2) disambiguation where
        candidate record pairs are selected based on hashes computed from coarse-grained (word)
        feature vectors.
        - Combine the Tokenizer tokendata and feature vectors of all states into a national file
        to be used in stage2 disambiguation.
        - Using the national Tokenizer tokendata and feature vectors, compute new (and longer)
        hashes, uniformly for the entire country. This will be used in stage2 to find candidate
        pairs at a national level.
    '''
    # National level data preparation:
    # Tokenize, vectorize and hashify all states using TokenizerName
    # for the blocking phase of stage II
    ltf = ['NAME']
    init.INIT_process_multiple_states(TokenizerClass=TokenizerName, list_tokenized_fields=ltf, num_procs=10, tokenize_kwargs={'export_normalized_attributes': False})

    # State level data preparation (for fine-grained intra state disambiguation)
    # Tokenize, vectorize and hashify all states using TokenizerNgram. See that class
    # for details.
    init.INIT_process_multiple_states(TokenizerClass=TokenizerNgram, num_procs=10)

    # National level data preparation:
    # Tokenize, vectorize and hashify all states using Tokenizer
    ltf = ['NAME', 'EMPLOYER', 'OCCUPATION']
    init.INIT_process_multiple_states(TokenizerClass=Tokenizer, list_tokenized_fields=ltf, num_procs=10)

    # combine the vectors and tokens from all states into the national data files.
    init.INIT_combine_state_tokens_and_vectors()

    # Combine all normalized attributes into a national file
    init.INIT_combine_normalized_attributes()

    # Using the national vectors and tokens, compute uniform national hashes
    init.INIT_compute_national_hashes(num_procs=10)













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




def test_view_vectors(state='delaware'):
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


def test_memory1():
    '''
    load the USA 20-char hashes to see memory use.
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



def test_identity_manager1():
    from disambiguation.core.Database import IdentityManager
    from disambiguation.core import utils

    idm = IdentityManager(state='USA')
    idm.fetch_dict_id_2_identity()
    print "Done"
    utils.time.sleep(60)
    return
    for identity, rids in idm.dict_identity_2_list_ids.iteritems():
        print identity, rids


def test_identity_manager2():
    '''
    Load identities and identity_adjacency and print out the connected components of the
    inter-cluster network.
    '''
    from disambiguation.core.Database import IdentityManager, FecRetrieverByID
    from disambiguation.core import utils
    import random
    from pprint import pprint

    idm = IdentityManager(state='USA')
    idm.fetch_dict_id_2_identity()
    idm.fetch_dict_identity_2_id()
    idm.fetch_dict_identity_adjacency()
    idm.load_dict_identity_2_identities()

    list_identities = sorted(idm.dict_identity_2_identities.keys(), key=lambda identity:len(idm.dict_identity_2_identities[identity]), reverse=True)
    # sample_identities = [random.choice(list_identities) for i in range(500)]
    sample_identities = list_identities

    list_ids = []

    for identity in sample_identities:
        dict_related_identities = idm.get_related_identities(identity)
        if not dict_related_identities: continue
        related_identities = [x for x, y in idm.get_related_identities(identity).iteritems() if (y[2] > 0 and y[0] == 0)]
        related_identities.append(identity)
        for related_identity in related_identities:
            ids = idm.get_ids(related_identity)
            list_ids += ids

    list_ids = list(set(list_ids))
    db = FecRetrieverByID('usa_combined_v1')
    db.retrieve(list_ids)
    list_of_records = db.getRecords()
    dict_records = {r.id:r for r in list_of_records}

    for identity in sample_identities:
        dict_related_identities = idm.get_related_identities(identity)
        if not dict_related_identities: continue
        related_identities = [x for x, y in idm.get_related_identities(identity).iteritems() if (y[2] > 0 and y[0] == 0)]
        related_identities_scores = [(x, y) for x, y in idm.get_related_identities(identity).iteritems() if (y[2] > 0 and y[0] == 0)]
        related_identities.append(identity)
        print "="*80
        pprint(related_identities_scores)
        for related_identity in related_identities:
            ids = idm.get_ids(related_identity)
            print "        " + "-"*20
            print "        " + related_identity
            print "        " + "-"*20
            for rid in ids:
                print "        ", dict_records[rid].toString()
            print
        print "\n\n"






def test_retriever_by_id():
    from disambiguation.core import Database
    retriever = Database.FecRetrieverByID('newyork_combined_v1')

    num_records = 10


    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    all_fields = list_tokenized_fields + list_auxiliary_fields

    db = FecRetriever(table_name='newyork_combined_v1',
                      query_fields=['id'],
                      limit=(0, num_records),
                      list_order_by='')
    db.retrieve()
    list_ids = [int(r.id) for r in db.getRecords()]



    retriever.retrieve(list_ids, all_fields)
    for r in retriever.getRecords():
        print r.id
    print list_ids







if __name__ == "__main__":

    INIT()

    DISAMBIGUATE_stage_1()

    DISAMBIGUATE_stage_2()
