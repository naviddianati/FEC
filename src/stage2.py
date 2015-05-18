'''
This module contains methods used for the second stage of disambiguation.
in this stage, we use combined national hashes and the state-level results
to identify pairs of records that are candidates for comparison but haven't
already been clustered together (will be mostly cross-state pairs). Then,
We divide these records into approximately independent partitions each of
which can be independently analyzed by a child process. This analysis consists
of performing pairwise comparisons for each specified pair and deciding about
whether to merge their corresponding clusters.
In making this decision, cluster level statistics will be used. That is,
information regarding name/employer/occupation/etc. frequencies obtained
from stage1 clusters are used.
'''

from disambiguation.core import utils, Disambiguator, Project, Tokenizer
from disambiguation.core import hashes
import disambiguation.config as config
from disambiguation.core import Database



def get_candidate_pairs(num_pairs, state='USA', recompute=False):
    '''
    Get pairs of record ids that are similar according
    to the national (combined) hashes, but aren't already
    linked at the state level.
    @param state: the state whose hashes will be used.
    @param num_pairs: number of new records to select for comparison
    @return: list of tuples of record ids.
    '''

    # file that either already contains, or will contain a set of candidate
    # pairs of record ids together with a "weight"
    candidate_pairs_file = config.candidate_pairs_file_template % state

    # Make sure the file already exists
    if not recompute:
        if utils.os.path.exists(candidate_pairs_file):
            print "Candidate pairs file already exists. Skipping."
            return

    # Number of adjacent hashes to log
    B = 3

    # number of times to shuffle the list of hashes.
    num_shuffles = 40

    # list of candidate pairs to be compared.
    list_pairs = []

    # Get the full sorted edgelist from national hashes.
    filename = config.hashes_file_template % (state, 'Tokenizer')

    print "Starting get_edgelist_from_hashes_file()..."
    edgelist = hashes.get_edgelist_from_hashes_file(filename, state, B, num_shuffles, num_procs=3, num_pairs=num_pairs)
    # edgelist.reverse()
    print "Done with get_edgelist_from_hashes_file()"
    print "Done fetching new pairs."

    # Export edgelist to file
    with open(candidate_pairs_file, 'w') as f:
        for edge in edgelist:
            f.write("%d %d %d\n" % (edge[0], edge[1], edge[2]))






def partition_records(num_partitions, state="USA"):
    '''
    Partition the set of records appearing in the pairs identified
    by L{get_candidate_pairs()} into num_partitions subsets
    with minimal inter-set links, and export the edgelists within
    each sibset to a separate file for each subset. The list of record
    pairs is read from a file generated by L{get_candidate_pairs()}.

    @param num_partitions: number of partitions to divide C{list_record_pairs} into.
    @status: only works if the giant component isn't "too" large.
    '''


    # file that either already contains, or will contain a set of candidate
    # pairs of record ids together with a "weight"
    candidate_pairs_file = config.candidate_pairs_file_template % state


    ig = utils.igraph
    with open(candidate_pairs_file) as f:
        g = ig.Graph.Read_Ncol(f, names=True, weights="if_present", directed=False)
#         print g.vcount(), g.ecount()



    list_components = g.components().subgraphs()

    # partition the list of all connected components into a list of
    # num_partition subsets such that the total number of nodes in each
    # partition is roughly constant.
    list_of_partitions = utils.partition_list_of_graphs(list_components, num_partitions)


    # Export the edgelist for each partition to a separate file.
    list_of_list_edges = [ [(g.vs[e.source]['name'], g.vs[e.target]['name'], str(int(e['weight']))) for g in partition for e in g.es] for partition in list_of_partitions]
    for counter, partition in enumerate(list_of_list_edges):
        with open(config.candidate_pairs_partitioned_file_template % (state, counter), 'w') as f:
            for edge in partition:
                f.write("%s %s %s\n" % edge)



def disambiguate_subsets_multiproc(num_partitions, state="USA", num_procs=12):
    '''
    Compare record pairs within each subset and save results.
    '''
    # List of filenames in which subsets of the edgelist are stored.
    list_filenames = [config.candidate_pairs_partitioned_file_template % (state, counter)\
                       for counter in range(num_partitions)]

    if num_procs == 1:
        for i, filename in enumerate(list_filenames):
            list_list_record_pairs = [worker_disambiguate_subset_of_edgelist(filename)]
    else:
        pool = utils.multiprocessing.Pool(num_procs)
        list_list_record_pairs = pool.map(worker_disambiguate_subset_of_edgelist, list_filenames)

    # Concatenate all sublists
    list_record_pairs = []
    while list_list_record_pairs:
        list_record_pairs += list_list_record_pairs.pop()



    # Create an IdentityManager instance, then given the record
    # paris just found, compute the identity_adjacency.
    idm = Database.IdentityManager('USA')
    idm.generate_dict_identity_adjacency(list_record_pairs, overwrite=True)
    idm.export_identities_adjacency()
    
    

def __get_customized_verdict(verdict, detailed_comparison_result):
    '''
    Function that makes the final judgment on the relationship between two
    records. It receives the output of the L{Record.compare} method consisting
    of a (preliminary) verdict and the detailed comparison results which
    provide information about the comparison results for the various fields
    of the records. Implement this method to define how these preliminary
    comparison results are to be interpreted ultimately. The output of this
    function will be used to determine the relationship between a pair of
    stage1 identities when a pair of records one from each are compared.
    The output of this function will determine if the two identities are
    to be merged, to be kept as irreconcilably separate, or as inconclusively
    similar.

    @param verdict: the verdict issued by L{Record.compare}. It can be True,
    False, or a large negative number. In the latter case, we have an
    irreconcilable difference such as different middle names.
    @param detailed_comparison_result: the result of comparisons of the
    various fields of the records. See L{Record._compare_THOROUGH} for the
    details.

    @return: a simple numerical verdict:
        - B{-1}: strongly inconsistent, such as when middle names are different.
        - B{0}: records are similar, but we don't have a conclusive verdict
        either way. We don't know that they're clearly a match, or clearly
        not a match.
        - B{1}: records are clearly a match.
        - B{None}: records are't similar, just ignore them.
    '''
    if verdict < 0 : return -1
    if verdict == False and  detailed_comparison_result['n'] == 3: return 0
    if verdict == True: return 1
    return None


def worker_disambiguate_subset_of_edgelist(filename):
    '''
    Disambiguate by comparing the record pairs in filename. To do this
    first extract the list of all record ids, generate a temp MySQL table
    with thos in it, then extract the full records from individual_contributions
    by joining it with the temp table.
    @note: the records need to be tokenized first, since record comparison relies
    on normalized names, etc.
    @param filename: filename in which on partition of the edgelist is stored.
    '''
    # Redirect the stdout of this process to a dedicated file.
    utils.sys.stdout = open("stdout-" + str(utils.os.getpid()) + ".out", "w")

    # What percentage of affiliation graph links to retain.
    percent_occupations = 50
    percent_employers = 50

    # Load Normalized token data
    normalized_tokendata_file = config.tokendata_file_template % ("USA", "Normalized")
    with open(normalized_tokendata_file) as f:
        tokendata_usa = utils.cPickle.load(f)



    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    all_fields = list_tokenized_fields + list_auxiliary_fields
    ig = utils.igraph
    with open(filename) as f:
        print "Loading partition of edgelist."
        g = ig.Graph.Read_Ncol(f, names=True, weights="if_present", directed=False)
        list_record_ids = [int(v['name']) for v in g.vs]

        print "Getting list_of_pairs."
        list_of_pairs = [(int(g.vs[e.source]['name']), int(g.vs[e.target]['name']))
            for e in sorted(g.es, key=lambda e:e['weight'], reverse=True)]

        print "Retrieving records for this partition from database."
        retriever = Database.FecRetrieverByID(config.MySQL_tablename_all_records)
        retriever.retrieve(list_record_ids, all_fields)
        list_of_records = retriever.getRecords()


        project = Project.Project(1)


        # This Tokenier object servers to tokenize the current
        # records. However, for token frequency data when compating
        # records, we use a different tokendata instance which is
        # for the entire country and is loaded from file: tokendata_usa
        tokenizer = Tokenizer.Tokenizer()
        project.tokenizer = tokenizer
        tokenizer.project = project
        tokenizer.setRecords(list_of_records)
        tokenizer.setTokenizedFields(list_tokenized_fields)

        # Insert the USA tokendata object into tokenizer.
        tokenizer.tokendata = tokendata_usa

        print "Tokenizing records..."
        tokenizer.tokenize()
        list_of_records = tokenizer.getRecords()

        print "Loading affiliation networks..."
        # Load affiliation networks
        G_employer = utils.loadAffiliationNetwork('USA' , 'employer', percent=percent_employers)
        if G_employer:
            for record in list_of_records:
                record.list_G_employer = [G_employer]
        else:
            utils.Log("EMPLOYER network not found.", "Warning")


        G_occupation = utils.loadAffiliationNetwork('USA' , 'occupation', percent=percent_occupations)
        if G_occupation:
            for record in list_of_records:
                record.list_G_occupation = [G_occupation]
        else:
            utils.Log("OCCUPATION network not found.", "Warning")


        print "Instantiating Disambiguator."
        D = Disambiguator.Disambiguator(list_of_records, vector_dimension=None, matching_mode='national', num_procs=1)
        project.D = D
        D.project = project
        D.tokenizer = tokenizer

        # This is the list which can be passed to Database.IdentityManager.generate_dict_identity_adjacency()
        list_record_pairs = []

        print "Running D.compare_list_of_pairs(list_of_pairs)"
        # D.compare_list_of_pairs is a generator: it yields the full results of
        # the pairwise comparisons and we are able to perform one last analysis and
        # decide what whether the math is a "no", "maybe" or "yes".
        for verdict, detailed_comparison_result, rid1, rid2 in  D.compare_list_of_pairs(list_of_pairs):
            final_result = __get_customized_verdict(verdict, detailed_comparison_result)
            list_record_pairs.append(((rid1, rid2), final_result))

        return list_record_pairs















from disambiguation.core import Person
from disambiguation.core import Record

def __update_tokendata_with_person(list_normalized_attrs, tokendata, list_virtual_records):
    '''
    Instantiate a Person object from the list of records, then
    for all normalized attributes, compute the dominant value
    among the records for that person, and using that dominant
    value, update the specified tokendata instance.
    '''
    person = Person.Person(list_virtual_records)
    # Loop through the normalized attributes and compute
    # the dominant value for the current identity
    for attr in list_normalized_attrs:
        dominant_attr = person.get_dominant_attribute(attr)
        if dominant_attr is None: continue
        token = tokendata.token_identifiers[attr][0], dominant_attr
        if token in tokendata.token_2_index:
            tokendata.token_counts[token] += 1
        else:
            tokendata.token_2_index[token] = tokendata.no_of_tokens
            tokendata.index_2_token[tokendata.no_of_tokens] = token
            tokendata.token_counts[token] = 1
            tokendata.no_of_tokens += 1


def compute_person_tokens():
    '''
    Using the ientities comptued in stage1 and the tokendata,
    compute the token frequencies at the person level, that is
    the number of identities with a given token rather than the
    number of records.
    For this, we instantiate a new TokenData object and update it
    with the token data. It will then be exported to a file. Since
    It is not based on tokenz produced by a C{Tokenizer} object, but
    rather using the normalized attributes, the file label contains
    "Normalized" rather than the Tokenizer class name.
    '''

    # TODO: add city and state to the contents of
    # config.normalized_attributes_file_template and
    # then use those here as well.
    list_normalized_attrs = [  # 'N_address',
                             'N_first_name',
                             'N_last_name',
                             'N_middle_name',
                             'N_full_name',
                             # 'N_zipcode',
                             'N_employer',
                             'N_occupation']

    # Instantiate a new TokenData object
    tokendata = Tokenizer.TokenData()

    # Customize the token identifiers for this TokenData
    # so they work for normalized attributes. In particular,
    # Add "N_full_name".
    # TODO: maybe all this can be done by subclassing Tokendata
    tokendata.token_identifiers = {'N_first_name':[2],
                                  'N_last_name':[1],
                                  'N_middle_name':[3],
                                  'N_full_name':[123],
                                  'N_zipcode':[4],
                                  'N_occupation':[6],
                                  'N_employer':[7]}

    # Load state normalized tokens one state
    # at a time and compute frequencies at the person level.
    for state in utils.states.dict_state_abbr:

        with open(config.normalized_attributes_file_template % state) as f:
            dict_normalized_attributes = utils.cPickle.load(f)
            print "Processing person-level token frequencies for state %s." % state

            # Load identities: instantiate an IdentityManager
            # instace.
            idm = Database.IdentityManager(state=state)
            idm.fetch_dict_identity_2_id()
            dict_identity_2_list_ids = idm.dict_identity_2_list_ids

            # loop through all identities in state, get their
            # records, generate person objects
            for identity, list_r_ids in dict_identity_2_list_ids.iteritems():

                # Here we treat the values of dict_normalized_attributes
                # which are dicts of normalized attr: value as "virtual
                # records". They will suffice for our purposes here.

                # Add an "id" key to each value of dict_normalized_attributes
                [dict_normalized_attributes[rid].update({"id":rid}) for rid in list_r_ids]

                # Add an "N_full_name" key to each value of dict_normalized_attributes
                for rid in list_r_ids:
                    tmp = dict_normalized_attributes[rid]
                    fullname = "%s %s %s" % (tmp['N_last_name'], tmp['N_middle_name'], tmp['N_first_name'])
                    dict_normalized_attributes[rid]['N_full_name'] = fullname

                list_virtual_records = [Record.Record(dict_normalized_attributes[rid]) for rid in list_r_ids]
                __update_tokendata_with_person(list_normalized_attrs, tokendata, list_virtual_records)

    # export to file.
    tokendata_file = config.tokendata_file_template % ("USA", 'Normalized')
    tokendata.save_to_file(tokendata_file)


