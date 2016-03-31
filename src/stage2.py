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
from disambiguation.core import Partitioner

np = utils.np
import sys, traceback




def get_candidate_pairs(num_pairs, state='USA', recompute=False, idm = None, mode='disambiguation'):
    '''
    Get pairs of record ids that are similar according
    to the national (combined) hashes, but aren't already
    linked at the state level.
    @param state: the state whose hashes will be used.
    @param num_pairs: number of new records to select for comparison
    @param idm: L{IdentityManager} instance to use. This is used to that
    records pairs both from the same identity are simply skipped.
    @param mode: can be 'disambiguation' or "bootstrapping". The results are
    written to different files depending on the value of this parameter.
    @return: list of tuples of record ids.
    '''

    # file that either already contains, or will contain a set of candidate
    # pairs of record ids together with a "weight"
    if mode == "disambiguation":
        #candidate_pairs_file = config.candidate_pairs_file_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_template
    elif mode == "bootstrapping":
        #candidate_pairs_file = config.candidate_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_bootstrapping_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_bootstrapping_template
    else:
        raise ValueError("ERROR: parameter 'mode' must be either 'disambiguation' or 'bootstrapping'.")
    
        

    # Make sure the file already exists
    if not recompute:
        if utils.os.path.exists(candidate_pairs_file):
            print "Candidate pairs file already exists. Skipping."
            return

    # Number of adjacent hashes to log
    B = 10

    # number of times to shuffle the list of hashes.
    num_shuffles = 40

    # list of candidate pairs to be compared.
    list_pairs = []

    # Get the full sorted edgelist from national hashes.
    filename = config.hashes_file_template % (state, 'Tokenizer')

    print "Starting get_edgelist_from_hashes_file()..."

    # NOTE: There is a particular case where relying on record edgelist fails us: consider the case
    # where there is one cluster with a single record and another with 100 records. Suppose that single
    # record is similar to quite a few of the records in the larger cluster. Using the hashing method
    # as we've done before will have the following result: the single record will definitely be found
    # close to *some* record from the large cluster in different permutations. Hoever, it may not end
    # up close to any one of them sonsistently enough that their proximity will be registered. 
    # So, instead of recording which records are proximare, we should register which *identities* are
    # proximate via their records.
    edgelist = hashes.get_edgelist_from_hashes_file(filename, state, B, num_shuffles, num_procs=3, num_pairs=num_pairs, idm = idm, prune = True)
    # edgelist.reverse()
    print "Done with get_edgelist_from_hashes_file()"
    print "Done fetching new pairs."

    # NOTE: new version produces edgelist of identities
    # Instead of records.
    # Export identity edgelist to file
    with open(candidate_S1_identity_pairs_file, 'w') as f:
        for edge in edgelist:
            #f.write("%s %s %d\n" % (edge[0], edge[1], edge[2]))
            f.write("%s %s\n" % (edge[0], edge[1]))




def partition_records(num_partitions, state="USA"):
    '''
    @deprecated: 
    Partition the set of records appearing in the pairs identified
    by L{get_candidate_pairs()} into num_partitions subsets
    with minimal inter-set links, and export the edgelists within
    each sibset to a separate file for each subset. The list of record
    pairs is read from a file generated by L{get_candidate_pairs()}.

    @param num_partitions: number of partitions to divide C{list_record_pairs} into.
    @status: only works if the giant component isn't "too" large. Otherwise,
    the partitions aren't balanced.
    '''


    # file that either already contains, or will contain a set of candidate
    # pairs of record ids together with a "weight"
    candidate_pairs_file = config.candidate_pairs_file_template % state


    ig = utils.igraph
    with open(candidate_pairs_file) as f:
        g = ig.Graph.Read_Ncol(f, names=True, weights="if_present", directed=False)


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

def __get_list_identity_pairs(list_record_pairs, idm):
    '''
    @deprecated: Now we directly find candidate identity pairs
    using the hashes, so we don't need to compute identity pairs
    from record pairs.
    Determine which identity pairs should be compared.
    Given a list of record id pairs, find all S1 identity pairs such
    that one of the record pairs has one record in one identity and
    another in the other.
    Each pair 
    @param list_record_pairs: list of 3-tuples C{(id1, id2, weight)}
    @param idm: an L{IdentityManager} instance
    @return: list of tuples C{(identity1, identity2)} where identity1 < identity2
    '''
    # Minimum total record-pair weight connecting 
    # the two identities. 
    # TODO: set this globally, perhaps in config.py
    threshold = 2

    set_identity_pairs = set()
    dict_identity_pairs = {}
    for rid1, rid2, weight in list_record_pairs:
        identity1 = idm.get_identity(rid1)
        identity2 = idm.get_identity(rid2)
        if not identity1 or not identity2: continue
        pair = tuple(sorted([identity1, identity2]))
        #set_identity_pairs.add(pair)
        try:
            dict_identity_pairs[pair] += weight
        except KeyError:
            dict_identity_pairs[pair] = weight

    # TODO: thresholding may need to be done differently,
    # for instance, based on the "average" weight of the
    # connecting record pairs.
    return [pair for pair,weight in dict_identity_pairs.iteritems() if weight > threshold]
    



def partition_S1_identities(num_partitions, state = "USA", idm = None, mode="disambiguation" ,recompute_identity_partitions=True):
    '''
    NEW: don't do graph partitioning.Do the most naive partitioning of the list of pairs.
    Replacing L{partition_records}, this method uses the record edgelist
    to identify which S1 "identities" are potential matches, and then partitions
    the identities--rather than the records--into minimally overlapping sets.
    The goal is to compare pairs of identities rather than records.
    This method has 2 main outputs: 
    1- C{config.candidate_S1_identity_pairs_partitioned_file_template} for each partition.
    This file conains a list of pairs of S1 identities that must be compared.
    2- C{config.candidate_list_records_partitioned_file_template} for each partition.
    This file contains a list of record ids associated with any of the identities found
    in the previous file.
    '''
    if not idm:
        # IdentityManager instance
        idm = Database.IdentityManager('USA')

    def load_identity_pair_partitions_from_file( candidate_S1_identity_pairs_partitioned_file_template, num_partitions):
        '''
        If idenetity paritions are already exported to files, use this function
        to load them rather than recompute them. Use the output to partition
        record ids. 
        '''
        list_of_list_edges = []
        for i in range(num_partitions):
            filename = candidate_S1_identity_pairs_partitioned_file_template % ( state, i)
                 
            with open(filename) as f1:
                try:
                    print "Loading list of identity pairs from file", filename
                    list_identity_pairs = [tuple(line.strip().split(' ')) for line in f1]
                    list_of_list_edges.append(list_identity_pairs)
                except Exception as e:
                    print "ERROR: ", e
                    print "Unable to load list of identity pairs from file ", filename
                    raise
        return list_of_list_edges
                

    # file that either already contains, or will contain a set of candidate
    # pairs of record ids together with a "weight"
    if mode == "disambiguation":
        candidate_pairs_file = config.candidate_pairs_file_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_template
    elif mode == "bootstrapping":
        candidate_pairs_file = config.candidate_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_bootstrapping_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_bootstrapping_template
    else:
        raise ValueError("ERROR: parameter 'mode' must be either 'disambiguation' or 'bootstrapping'.")
    

    # NOTE: in the new version, we start with the candidate
    # identity pairs, so we don't need to find them from candidate
    # record pairs first.



    if recompute_identity_partitions:
        # Divide the set of S1 identities into roughly equally sized components.
        ig = utils.igraph
        with open(candidate_S1_identity_pairs_file) as f:
            list_of_edges = [tuple(line.strip().split(' ')) for line in f]
            g_fec = ig.Graph.TupleList(list_of_edges)
        ##    g = ig.Graph.Read_Ncol(f, names=True, weights="if_present", directed=False)
        ##print "S1 identity candidates read from file."
        ##print "Graph of S1 identities:"
        ##print "   vcount: ", g.vcount()
        ##print "   ecount: ", g.ecount()
        ##print "   density:", float(g.ecount())/g.vcount()


        #list_components = g.components().subgraphs()
        ##list_components = g.community_fastgreedy().as_clustering().subgraphs()
        ##print "List of identity pairs divided into partitions."

        # partition the list of all connected components into a list of
        # num_partition subsets such that the total number of nodes in each
        # partition is roughly constant.
        ##list_of_partitions = utils.partition_list_of_graphs(list_components, num_partitions)
        ##print "lengths of partitions:" , [sum([g.vcount() for g in partition]) for partition in list_of_partitions]
        ##print "max size of graphs in partitions:" , [max([g.vcount() for g in partition]) for partition in list_of_partitions]

        
        # Export the identity edgelist for each partition to a separate file.
        # This is an edgelist between S1 identities
        print "Saving identity pair partitions to separate files..."
        ## list_of_list_edges = [ [(g.vs[e.source]['name'], g.vs[e.target]['name']) for g in partition for e in g.es] for partition in list_of_partitions]
        
        
        
        
        # VERY INEFFICEINT AS IT TURNS OUT
        # list_of_list_edges = utils.chunks(list_of_edges, num_partitions)
        
        # New solution using the Partitioner class
        partitioner = Partitioner.Partitioner(g_fec, num_partitions = num_partitions)
        #nt.tic()
        partitioner.partition()
        #nt.toc()
        partitioner.print_stats()
        
        for partition_number in range(len(partitioner.list_edgelists)):
            with open(candidate_S1_identity_pairs_partitioned_file_template % (state, partition_number), 'w') as f:
                for edge in partitioner.get_named_edgelist(partition_number):
                    f.write("%s %s\n" % edge)
        list_of_list_edges = partitioner.list_edgelists
        print "Done."



    else:
        # NOTE: only if the identity pair partitions are already written
        # to file. Don't recompute the identity pair partitions, but load
        # them from file, so that record id partitions can be quickly computed.
        # and exported to file.
        list_of_list_edges = load_identity_pair_partitions_from_file( candidate_S1_identity_pairs_partitioned_file_template, num_partitions)    



    # For each partition, export a list of all record ids necessary
    # for processing that partition. These are all record ids associated
    # with any of the identities in the partition.
    for counter, partition in enumerate(list_of_list_edges):
        set_record_ids = set()
        for edge in partition:
            identity1, identity2 = edge
            set_record_ids.update(set(idm.get_ids(identity1)))
            set_record_ids.update(set(idm.get_ids(identity2)))

        with open(candidate_list_records_partitioned_file_template % (state, counter), 'w') as f:
            for r_id in set_record_ids:
                f.write('%d\n' % r_id)








def partition_S1_identities_OLD(num_partitions, state = "USA", idm = None, mode="disambiguation" ,recompute_identity_partitions=True):
    '''
    SLOW GRAPH PARTITIONING>
    Replacing L{partition_records}, this method uses the record edgelist
    to identify which S1 "identities" are potential matches, and then partitions
    the identities--rather than the records--into minimally overlapping sets.
    The goal is to compare pairs of identities rather than records.
    This method has 2 main outputs: 
    1- C{config.candidate_S1_identity_pairs_partitioned_file_template} for each partition.
    This file conains a list of pairs of S1 identities that must be compared.
    2- C{config.candidate_list_records_partitioned_file_template} for each partition.
    This file contains a list of record ids associated with any of the identities found
    in the previous file.
    '''
    if not idm:
        # IdentityManager instance
        idm = Database.IdentityManager('USA')

    def load_identity_pair_partitions_from_file( candidate_S1_identity_pairs_partitioned_file_template, num_partitions):
        '''
        If idenetity paritions are already exported to files, use this function
        to load them rather than recompute them. Use the output to partition
        record ids. 
        '''
        list_of_list_edges = []
        for i in range(num_partitions):
            filename = candidate_S1_identity_pairs_partitioned_file_template % ( state, i)
                 
            with open(filename) as f1:
                try:
                    print "Loading list of identity pairs from file", filename
                    list_identity_pairs = [tuple(line.strip().split(' ')) for line in f1]
                    list_of_list_edges.append(list_identity_pairs)
                except Exception as e:
                    print "ERROR: ", e
                    print "Unable to load list of identity pairs from file ", filename
                    raise
        return list_of_list_edges
                

    # file that either already contains, or will contain a set of candidate
    # pairs of record ids together with a "weight"
    if mode == "disambiguation":
        candidate_pairs_file = config.candidate_pairs_file_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_template
    elif mode == "bootstrapping":
        candidate_pairs_file = config.candidate_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_bootstrapping_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_bootstrapping_template
    else:
        raise ValueError("ERROR: parameter 'mode' must be either 'disambiguation' or 'bootstrapping'.")
    

    # NOTE: in the new version, we start with the candidate
    # identity pairs, so we don't need to find them from candidate
    # record pairs first.



    if recompute_identity_partitions:
        # Divide the set of S1 identities into roughly equally sized components.
        ig = utils.igraph
        with open(candidate_S1_identity_pairs_file) as f:
            g = ig.Graph.Read_Ncol(f, names=True, weights="if_present", directed=False)
        print "S1 identity candidates read from file."
        print "Graph of S1 identities:"
        print "   vcount: ", g.vcount()
        print "   ecount: ", g.ecount()
        print "   density:", float(g.ecount())/g.vcount()


        #list_components = g.components().subgraphs()
        list_components = g.community_fastgreedy().as_clustering().subgraphs()
        print "List of identity pairs divided into partitions."

        # partition the list of all connected components into a list of
        # num_partition subsets such that the total number of nodes in each
        # partition is roughly constant.
        list_of_partitions = utils.partition_list_of_graphs(list_components, num_partitions)
        print "lengths of partitions:" , [sum([g.vcount() for g in partition]) for partition in list_of_partitions]
        print "max size of graphs in partitions:" , [max([g.vcount() for g in partition]) for partition in list_of_partitions]

        
        # Export the identity edgelist for each partition to a separate file.
        # This is an edgelist between S1 identities
        print "Saving identity pair partitions to separate files..."
        list_of_list_edges = [ [(g.vs[e.source]['name'], g.vs[e.target]['name']) for g in partition for e in g.es] for partition in list_of_partitions]
        for counter, partition in enumerate(list_of_list_edges):
            with open(candidate_S1_identity_pairs_partitioned_file_template % (state, counter), 'w') as f:
                for edge in partition:
                    f.write("%s %s\n" % edge)
        print "Done."



    else:
        # NOTE: only if the identity pair partitions are already written
        # to file. Don't recompute the identity pair partitions, but load
        # them from file, so that record id partitions can be quickly computed.
        # and exported to file.
        list_of_list_edges = load_identity_pair_partitions_from_file( candidate_S1_identity_pairs_partitioned_file_template, num_partitions)    



    # For each partition, export a list of all record ids necessary
    # for processing that partition. These are all record ids associated
    # with any of the identities in the partition.
    for counter, partition in enumerate(list_of_list_edges):
        set_record_ids = set()
        for edge in partition:
            identity1, identity2 = edge
            set_record_ids.update(set(idm.get_ids(identity1)))
            set_record_ids.update(set(idm.get_ids(identity2)))

        with open(candidate_list_records_partitioned_file_template % (state, counter), 'w') as f:
            for r_id in set_record_ids:
                f.write('%d\n' % r_id)



def data_generator(num_partitions, idm, mode = 'disambiguation', state = 'USA'):
    '''
    Used in disambiguate_subsets_multiproc() 
    Generator that yields the data necessary for each 
    data partition, to be used in the child workers.'''

    ig = utils.igraph
    if mode == "disambiguation":
        bootstrap = False
        candidate_pairs_file = config.candidate_pairs_file_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_template
    elif mode == "bootstrapping":
        bootstrap = True
        candidate_pairs_file = config.candidate_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_bootstrapping_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_bootstrapping_template
    else:
        raise ValueError("ERROR: parameter 'mode' must be either 'disambiguation' or 'bootstrapping'.")
    # generate a dict_identities for each partition and pass it to the worker method
    # List of data packages ((filename1, filename2), dict_idenities)
    # to be sent to workers.
    for counter in range(num_partitions):
        print "Opening data files for partition ", counter
        filename1 = candidate_S1_identity_pairs_partitioned_file_template % (state, counter)
        filename2 = candidate_list_records_partitioned_file_template % (state, counter)
        with open(filename1) as f:
            g = ig.Graph.Read_Ncol(f, names=True, weights="if_present", directed=False)
            print "    Graph loaded."
            list_identities =  [v['name'] for v in g.vs]
            dict_identities = {identity: idm.get_ids(identity) for identity in list_identities}
            print "length of dict_identities: ", len(dict_identities)
            yield ((filename1, filename2), dict_identities, counter)




def distribute(data_generator, num_procs, worker_fcn):
    # create a list of pipes
    list_pipes = [utils.multiprocessing.Pipe() for i in range(num_procs)]

    # list of processes. Each receives a pipe as argument.
    list_processes = [utils.multiprocessing.Process(target = worker_fcn, args = [list_pipes[i]]) for i in range(num_procs)]

    list_results  = []

    # which process is idle. possible values: 'idle' or 'busy'
    list_status = ['idle' for i in range(num_procs)]

    for process in list_processes:
        process.start()

    # Number of results received.
    num_results = 0


    # Initialize with Infinity. It will be determined later.
    num_total_batches = 10000000


    counter_batches = 0
    while True:
        utils.time.sleep(0.1)
        for pid, pipe in enumerate(list_pipes):
            p_parent, p_child = pipe

            # If child has sent results, receive them and
            # mark child status to 'idle', ready to send
            # next batch of data.
            if list_status[pid] == 'busy' and p_parent.poll():  # there are incoming results
                response = p_parent.recv()
                list_results.append(response)
                num_results += 1
                print "Result no %d received..." % num_results 
                #print response
                list_status[pid] = 'idle'

            # If child is idle, whether bc we just received
            # results from it or if it hasn't been sent any
            # data yet at all, send it data.
            if list_status[pid] == 'idle':
                try:
                    p_parent.send(data_generator.next())
                    counter_batches += 1
                    print "Batch no %d started............" % counter_batches
                    list_status[pid] = 'busy'
                except StopIteration:
                    num_total_batches = counter_batches
                    print pid, 'num_total_batches: ', num_total_batches, '    num_results:', num_results
                    print list_status
                    p_parent.send('done') #signal child to terminate
                    list_status[pid] = 'dead'
                    print 'kill signal successfully sent to child.'
                    print
                    
        if num_results == num_total_batches:
            print "All results received, exiting main loop..."
            break

    print "Sending final kill signal to all child processes..."
    for p_parent, p_child in list_pipes:
        p_parent.send('done')


    print "Joining all subprocesses..."
    for process in list_processes:
        process.join()

    for pipe in list_pipes:
        pipe[0].close()
        pipe[1].close()
        
    for process in list_processes:
        process.terminate()


    return list_results


def disambiguate_subsets_multiproc(num_partitions, state="USA", num_procs=12, idm = None, mode = "disambiguation"):
    '''
    NEW: compare pairs of L{Person} objects derived from the candidate identity
    pairs withing each partition.

    Compare record pairs within each subset and save results. Can be done
    with 1 process or multiple processes. Uses the worker function
    L{worker_disambiguate_subset_of_edgelist}.
    @param mode: String. Whether this run is for bootstrapping stage II 
    disambiguation or the real deal. If it's boostrapping, the results of identity
    pair comparisons will be written to file, and the database tables won't be
    written. The results can be read from file and then used for the actual
    stage II disambiguation.

    @requires: The full stage1 'identities' table
    @requires: List of candidate record pairs already partitioned and written
    to separate files.
    '''
    

    if not idm:
        # IdentityManager instance
        idm = Database.IdentityManager('USA')
        
    ig = utils.igraph

    if mode == "disambiguation":
        bootstrap = False
    elif mode == "bootstrapping":
        bootstrap = True
    else:
        raise ValueError("ERROR: parameter 'mode' must be either 'disambiguation' or 'bootstrapping'.")


    # Each element in this list is itself a list.
    # For the format of the data that will be placed in these lists
    # remains the same as the old version.
    #list_list_identity_pairs = []
    list_list_identity_pairs =  distribute(data_generator(num_partitions, idm,  mode, state), num_procs, wrapper_worker_disambiguate_subset_of_identities_edgelist)


    #if num_procs == 1:
    #    for tuple_filenames, dict_identities, workerid in list_data:
    #        list_list_identity_pairs.append(wrapper_worker_disambiguate_subset_of_identities_edgelist((tuple_filenames, dict_identities, 0)))
    #else:
    #    print "Starting worker pool..."
    #    pool = utils.multiprocessing.Pool(num_procs, maxtasksperchild=1)
    #    list_list_identity_pairs = pool.map(wrapper_worker_disambiguate_subset_of_identities_edgelist, list_data)
    #    pool.close()
    #    pool.join()


    
    print "All worker results returned."


    # Concatenate all sublists
    list_identity_pairs = []
    while list_list_identity_pairs:
        list_identity_pairs += list_list_identity_pairs.pop()
    print "Combined all list_identity_pairs."

    # Create an IdentityManager instance, then given the record
    # pairs just found, compute the identity_adjacency.
    #idm = Database.IdentityManager('USA')

    print "Running idm.generate_dict_identity_adjacency."

    # File into which the bootstrapping results will be
    # written if boostrap is True.
    
    export_file = config.S2_bootstrap_results_file if bootstrap else config.S2_identity_comparison_results_file

    verdict_authority =  VerdictAuthorityBase() if bootstrap else VerdictAuthority()

    idm.generate_dict_identity_adjacency(list_identity_pairs, overwrite=True, export_file = export_file, verdict_authority = verdict_authority)
    
    # TODO: implement
    # Generate a set of super-identities, i.e. a sorted tuples of 
    # S1 identities that are linked and have no middle name conflicts.
    idm.deduce_linked_identities()

    # Export the deduced set of s1 identities to a MySQL table
    idm.export_linked_identities()

    # TODO: impolemenet a new get_linked_identities method that
    # uses data loaded from the linked_identities table and then
    # simply returns the list of all linked S1 identities.



    print "Done"

    
    if not bootstrap:
        print "Exporting identities_adjaceny.."
        idm.export_identities_adjacency()
        print "Done."    
    
        print "Exporting linked identities to csv file..."
        # TODO: reimplement this to use the newly generated
        # idm.dict_linked_identities
        idm.export_linked_identities_csv()
        print "Done."

        print "Exporting related identities to csv file..."
        idm.export_related_identities_csv()
        print "Done."






class VerdictAuthorityBase():
    def __init__(self):
        pass
    def verdict(self, value):
        return 1
        
class VerdictAuthority(VerdictAuthorityBase):
    '''
    @ivar inds_o_3: list of tuples C{(f1,f2)} where f1 and f2
    are the acceptable name frequencies (with and without middle name)
    when occupations are linked. This list must be converted to a set 
    for use by the verdict function.
    @ivar inds_e_3: list of tuples C{(f1,f2)} where f1 and f2
    are the acceptable name frequencies (with and without middle name)
    when employers are linked. This list must be converted to a set 
    for use by the verdict function.
    @ivar inds_o_4: list of tuples C{(f1,f2)} where f1 and f2
    are the acceptable name frequencies (with and without middle name)
    when occupations are identical. This list must be converted to a set 
    for use by the verdict function.
    @ivar inds_e_4: list of tuples C{(f1,f2)} where f1 and f2
    are the acceptable name frequencies (with and without middle name)
    when employers are identical. This list must be converted to a set 
    for use by the verdict function.
    '''
    def __init__(self):

        filename_inds_n_4_o_4 = config.filename_inds_n_4_o_4
        filename_inds_n_4_e_4 = config.filename_inds_n_4_e_4
        filename_inds_n_4_o_3 = config.filename_inds_n_4_o_3
        filename_inds_n_4_e_3 = config.filename_inds_n_4_e_3
        filename_inds_n_4_o_2 = config.filename_inds_n_4_o_2
        filename_inds_n_4_e_2 = config.filename_inds_n_4_e_2

        filename_inds_n_3 = config.filename_inds_n_3
        
        try:
            with open(filename_inds_n_4_o_4) as f:
                tmp = utils.json.load(f)
                self.inds_n_4_o_4 = set([tuple(x) for x in tmp])

            with open(filename_inds_n_4_e_4) as f:
                tmp = utils.json.load(f)
                self.inds_n_4_e_4 = set([tuple(x) for x in tmp])

            with open(filename_inds_n_4_o_3) as f:
                tmp = utils.json.load(f)
                self.inds_n_4_o_3 = set([tuple(x) for x in tmp])

            with open(filename_inds_n_4_e_3) as f:
                tmp = utils.json.load(f)
                self.inds_n_4_e_3 = set([tuple(x) for x in tmp])

            with open(filename_inds_n_4_o_2) as f:
                tmp = utils.json.load(f)
                self.inds_n_4_o_2 = set([tuple(x) for x in tmp])

            with open(filename_inds_n_4_e_2) as f:
                tmp = utils.json.load(f)
                self.inds_n_4_e_2 = set([tuple(x) for x in tmp])


            with open(filename_inds_n_3) as f:
                tmp = utils.json.load(f)
                self.inds_n_3 = tmp 

        except Exception as e:
            print "ERROR: unable to instantiate VerdictAuthority."
            raise e
        print "VerdictAuthority instantiated."

    def verdict(self, result):
        '''
        Issue a verdict on an identity pair based on their
        detailed comparison results.
        @param result: a list of three values. The values are
        C{result_name}, C{result_occupation} and C{result_employer}
        Each one is a tuple. For the details of the result formats
        see L{Person.compare}.

        @return: -1, 0 or 1. -1 means there is a middle name conflict
        that could not be rule out as an error based on exact affiliation
        matches. 0 means not a match, but not a middle name conflict either.
        1 means a match.
        '''
        
        result_name, result_occupation, result_employer = result
        
        # If names don't match return 0 unless they have
        # a middle namd conflict (-1) in which case continue
        # analyzing.
        if result_name[0] < 3 and result_name[0] > -1: return 0

        if result_occupation[0] < 2 and result_employer[0] < 2: return 0

        
        # f1 is with middle name, f2 is without
        f1, f2 = result_name[1]
        f1 = 0 if f1 is None else f1
        f2 = 0 if f2 is None else f2

        # If there is middle name conflict, accept only if
        # at least one of employer and occupation is an exact
        # match and the other is at least linked.
        if result_name[0] == -1:
            r_e, r_o = result_employer[0], result_occupation[0]
            if ((r_e == 4) and (r_o >= 3)) or  ((r_o == 4) and (r_e >= 3)):
                return 1
            else:
                return -1

        
        if result_name[0] == 4:
            scale = 3
            if result_occupation[0] == 4:
                if (f1,f2) in self.inds_n_4_o_4: return 4 * scale #1
            if result_employer[0] == 4:
                if (f1,f2) in self.inds_n_4_e_4: return 4 * scale #1

            if result_occupation[0] == 3:
                if (f1,f2) in self.inds_n_4_o_3: return 3 * scale #1
            if result_employer[0] == 3:
                if (f1,f2) in self.inds_n_4_e_3: return 3 * scale #1

            if result_occupation[0] == 2:
                if (f1,f2) in self.inds_n_4_o_2: return 2 * scale #1
            if result_employer[0] == 2:
                if (f1,f2) in self.inds_n_4_e_2: return 2 * scale #1

        elif result_name[0] == 3:
            scale = 2
            if result_occupation[0] == 4:
                if f2 < self.inds_n_3['o_4']: return 4 * scale #1
            if result_employer[0] == 4:
                if f2 < self.inds_n_3['e_4']: return 4 * scale #1

            if result_occupation[0] == 3:
                if f2 < self.inds_n_3['o_3']: return 3 * scale #1
            if result_employer[0] == 3:
                if f2 < self.inds_n_3['e_3']: return 3 * scale #1

            if result_occupation[0] == 2:
                if f2 < self.inds_n_3['o_2']: return 2 * scale #1
            if result_employer[0] == 2:
                if f2 < self.inds_n_3['e_2']: return 2 * scale #1
            
        # if none worked, return 0
        return 0







def disambiguate_subsets_multiproc_OLD(num_partitions, state="USA", num_procs=12):
    '''
    Compare record pairs within each subset and save results. Can be done
    with 1 process or multiple processes. Uses the worker function
    L{worker_disambiguate_subset_of_edgelist}.
    @requires: The full stage1 'identities' table
    @requires: List of candidate record pairs already partitioned and written
    to separate files.
    '''
    # List of filenames in which subsets of the edgelist are stored.
    list_filenames = [config.candidate_pairs_partitioned_file_template % (state, counter)\
                       for counter in range(num_partitions)]

    list_list_record_pairs = []
    if num_procs == 1:
        for i, filename in enumerate(list_filenames):
            list_list_record_pairs.append(worker_disambiguate_subset_of_edgelist(filename))
    else:
        pool = utils.multiprocessing.Pool(num_procs)
        list_list_record_pairs = pool.map(worker_disambiguate_subset_of_edgelist, list_filenames)
        pool.close()
        pool.terminate()
    
    print "All worker results returned."
    # Concatenate all sublists
    list_record_pairs = []
    while list_list_record_pairs:
        list_record_pairs += list_list_record_pairs.pop()
    print "Combined all list_record_pairs."

    # Create an IdentityManager instance, then given the record
    # pairs just found, compute the identity_adjacency.
    idm = Database.IdentityManager('USA')

    print "Running idm.generate_dict_identity_adjacency."
    idm.generate_dict_identity_adjacency(list_record_pairs, overwrite=True)
    print "Done"
    print "Exporting identities_adjaceny.."
    idm.export_identities_adjacency()
    idm.export_linked_identities_csv()
    idm.export_related_identities_csv()









def __get_customized_verdict(detailed_comparison_result):
    '''
    @deprecated: the final verdict is now managed by L{VerdictAuthority}

    Function that makes the final judgment on the relationship between two
    identities. It receives the output of the L{Person.compare} method consisting
    of the detailed comparison results which give the best match scores for
    between all record pairs of the two identities for the different fields. 
    Implement this method to define how these preliminary
    comparison results are to be interpreted ultimately. The output of this
    function will be used to determine the relationship between a pair of
    stage1 identities when all pairs of records one from each are compared.
    The output of this function will determine if the two identities are
    to be merged, to be kept as irreconcilably separate, or as inconclusively
    similar.

    As an exmple, the fullname frequencies can be used.

    @param detailed_comparison_result: The comparison results returned by L{Record.compare}.
    It is a tuple (currently) like C{(max_name, max_occupation, max_employer)}
    where the entries are the return values of L{Record}'s comparison methods
    for the respective fields. See L{Person.compare} for more details.

    @return: a simple numerical verdict:
        - B{-1}: strongly inconsistent, such as when middle names are different.
        - B{0}: records are similar, but we don't have a conclusive verdict
        either way. We don't know that they're clearly a match, or clearly
        not a match.
        - B{>0}: records are a match.
        - B{None}: records are't similar, just ignore them.
    '''
    # TODO: implement! Right now, it simply returns the results 3-tuple
    return detailed_comparison_result

    if verdict < 0 : return -1
    if verdict == False and  detailed_comparison_result['n'][0] >= 3: return 0
    if verdict == True: 
        # This is a tuple (freq_fullname_with_middlename, freq_fullname_without_middlename)
        # This tuple definitely has a value since True verdict is only issued when
        # match_code is 3 or 4.
        # print detailed_comparison_result
        freq1, freq2 = detailed_comparison_result['n'][1]

        try:
            score = 1./(freq1 + freq2)
        except:
            # If we return 0, it won't be any different
            # from verdict = False
            score = 0.5
        return score

    return None


def wrapper_worker_disambiguate_subset_of_identities_edgelist(pipe):
    '''
    A wrapper to the worker methods that allows exceptions
    in suvprocesses to be captured and displayed.
    '''
    try:
        return worker_disambiguate_subset_of_identities_edgelist(pipe)
    except:
        raise Exception("".join(traceback.format_exception(*sys.exc_info())))
        

def task_process_one_partition(data, tokendata_usa):
    '''
    This is the function that processes one data partition at a time.
    This function will be called multiple times withing a child process.
    The point is that the most expensive data reading operations are done
    OUTSIDE this function, only once by each child process. Then that
    data is used for all partitions processed by the child process.
    '''
    # What percentage of affiliation graph links to retain.
    percent_occupations = config.percent_occupations_S2
    percent_employers = config.percent_employers_S2

    tuple_filenames, dict_identities, workerid = data
    print "Processing partition ", workerid

    # Recover passed filenames
    candidate_S1_identity_pairs_partitioned_file, candidate_list_records_partitioned_file = tuple_filenames



    list_tokenized_fields = ['NAME', 'CONTRIBUTOR_ZIP', 'ZIP_CODE', 'CONTRIBUTOR_STREET_1', 'CITY', 'STATE', 'EMPLOYER', 'OCCUPATION']
    list_auxiliary_fields = ['TRANSACTION_DT', 'TRANSACTION_AMT', 'CMTE_ID', 'ENTITY_TP', 'id']
    all_fields = list_tokenized_fields + list_auxiliary_fields
    ig = utils.igraph
    
    # Load list of record ids
    with open(candidate_list_records_partitioned_file) as f1:
        try:
            print "Loading list of record ids from file", candidate_list_records_partitioned_file 
            list_record_ids = [int(line.strip()) for line in f1]
        except Exception as e:
            print "ERROR: ", e
            print "Unable to load list of record ids from file ", candidate_list_records_partitioned_file 
            raise

        print "Number of records loaded: ", len(list_record_ids)
        print "Done."

        list_of_pairs = [] # probably unnecessary.

        # Load list of identity pairs.
        with open(candidate_S1_identity_pairs_partitioned_file) as f2:
            print "Loading partition of edgelist."
            g = ig.Graph.Read_Ncol(f2, names=True, weights="if_present", directed=False)
            list_identities =  [v['name'] for v in g.vs]

            print "Getting list_of_identity_pairs."
            # Identity pairs don't come with weights. Can't sort.
            list_of_identity_pairs = [(g.vs[e.source]['name'], g.vs[e.target]['name']) for e in g.es]
                #for e in sorted(g.es, key=lambda e:e['weight'], reverse=True)]



        print "Retrieving records for this partition from database."

        pause_time = workerid * 2 
        utils.time.sleep(pause_time)

        retriever = Database.FecRetrieverByID(config.MySQL_table_usa_combined)
        t1 = utils.time.time()
        retriever.retrieve(list_record_ids, all_fields)
        list_of_records = retriever.getRecords()
        t2 = utils.time.time()
        print "records retrieved in {} seconds.".format(int(t2-t1))

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


        print "Tokenizing records..."
        tokenizer.tokenize(export_tokendata=False,export_vectors=False,export_normalized_attributes=False)
        list_of_records = tokenizer.getRecords()

        # Insert the USA tokendata object into tokenizer.
        tokenizer.tokendata = tokendata_usa

        for r in list_of_records:
            r.tokendata = tokendata_usa

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
        list_result_identity_pairs = []



        print "Running D.compare_list_of_identity_pairs(list_of_identity_pairs, dict_identities )"
        # D.compare_list_of_identity_pairs is a generator: it yields the full results of
        # the pairwise identity comparisons and we are able to perform one last analysis and
        # decide what whether the math is a "no", "maybe" or "yes".
        # NOTE: mostly done, Person.compare() can still be refined.
        for detailed_comparison_result, identity1, identity2 in \
              D.compare_list_of_identity_pairs(list_of_identity_pairs, dict_identities):
            list_result_identity_pairs.append(((identity1, identity2), detailed_comparison_result))



        return list_result_identity_pairs








def worker_disambiguate_subset_of_identities_edgelist(pipe):
    '''
    Disambiguate by comparing the S1 identity pairs in provided file, using
    all their records as provided by the other file.

    @param tuple_filenames: a tuple consisting of two strings. The first
    string is the filename containing a list of pairs of S1 identities.
    The second string is the filename containing a list of all the records
    associated with any of the identities present in the first file.
    @param dict_identities: a dict that maps each of the S1 identities passed
    to this method to a list of its associated records. We pass this dict
    in order to avoid instantiating an IdentityManager for each process running
    this method.
    
    The record comparisons performed in this function use the C{"national"} C{method_id}.
    This comparison method is slightly more lax than the stage1 comparisons, but we make
    additional judgments based on token frequencies here in L{__get_customized_verdict}.
    
    @note: the records need to be tokenized first, since record comparison relies
    on normalized names, etc.

    @return: list_identity_pairs: a list where each element looks like 
    C{((identity1, identity2), final_result)}.
    '''

    # Redirect the stdout of this process to a dedicated file.
    utils.sys.stdout = open("stdout-" + str(utils.os.getpid()) + ".out", "w", 0)
            
    # Load Normalized token data
    # This is a very expensive operation, so it must be done
    # once in the worker process, NOT once for each partition.
    # Each worker process loads this once, then uses it to process
    # multiple data partitions.
    normalized_tokendata_file = config.tokendata_file_template % ("USA", "Normalized")
    with open(normalized_tokendata_file) as f:
        tokendata_usa = utils.cPickle.load(f)

    
    ####################################################
    # BEGIN CHILD MAIN LOOP
    ####################################################
    p_parent, p_child = pipe
    pid = utils.os.getpid() 
    while True:
        utils.time.sleep(0.5)
        if p_child.poll():
            data = p_child.recv()
                
            # If empty packet received, exit loop,
            # terminate worker.
            if data=='done':
                print "Kill signal received, terminating child process."
                break

            result = task_process_one_partition(data, tokendata_usa)
            print "Child process results generated. Being sent back..."
            p_child.send(result)
            print "Results sent back successfully."

    print "left porocess main loop."
    ####################################################
    # END CHILD MAIN LOOP
    ####################################################












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
                             'N_full_name_withmiddle',
                             'N_full_name_withoutmiddle',
                             # 'N_zipcode',
                             'N_employer',
                             'N_occupation']

    # Instantiate a new TokenData object
    tokendata = Tokenizer.TokenData()

    # Customize the token identifiers for this TokenData
    # so they work for normalized attributes. In particular,
    # Add "N_full_name".
    # TODO: maybe all this can be done by subclassing Tokendata
    # Here, as in Tokenizer, occupation and employer get the
    # same identifier.
    tokendata.token_identifiers = {'N_first_name':[2],
                                  'N_last_name':[1],
                                  'N_middle_name':[3],
                                  'N_full_name':[123],
                                  'N_full_name_withmiddle':[123],
                                  'N_full_name_withoutmiddle':[123],
                                  'N_zipcode':[4],
                                  'N_occupation':[67],
                                  'N_employer':[67]}

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
                    # For each record, we compute the full name in two different
                    # ways: with and without the middle name. Both are counted 
                    # toward the fullname in the token frequency dictionary. 
                    # NOTE: if there is a fullname_withmiddle, then it is 
                    # necessarily different from fullname_withoutmiddle, so
                    # it won't be double counted.
                    if tmp['N_middle_name']:
                        fullname_withmiddle = "%s|%s|%s" % (tmp['N_last_name'], tmp['N_middle_name'], tmp['N_first_name'])
                    else:
                        fullname_withmiddle = ''
 
                    fullname_withoutmiddle = "%s||%s" % (tmp['N_last_name'], tmp['N_first_name'])
                    dict_normalized_attributes[rid]['N_full_name_withmiddle'] = fullname_withmiddle
                    dict_normalized_attributes[rid]['N_full_name_withoutmiddle'] = fullname_withoutmiddle

                list_virtual_records = [Record.Record(dict_normalized_attributes[rid]) for rid in list_r_ids]
                __update_tokendata_with_person(list_normalized_attrs, tokendata, list_virtual_records)


    # export to file.
    tokendata_file = config.tokendata_file_template % ("USA", 'Normalized')
    tokendata.save_to_file(tokendata_file)






def split_dict_identity_ids(dict_identity_2_list_ids, split_prob = 0.8):
    '''
    Take a C{dict_identity_2_list_ids} such as the one found
    in L{IdentityManager} and split the identities at random.
    Also compute the corresponding C{dict_id_2_identity} and return
    as well.
    '''

    import numpy as np
    # split_prob = 0.8
    dict_identity_2_list_ids_split = {}
    dict_id_2_identity_split = {}
    for identity, list_ids in dict_identity_2_list_ids.iteritems():
        x = len(list_ids)
        if np.random.rand() < split_prob and x > 1:          
            y = x / 2
            identity_sub_1 = identity + "|1" 
            identity_sub_2 = identity + "|2"
            dict_identity_2_list_ids_split[identity_sub_1] = list_ids[:y]
            dict_identity_2_list_ids_split[identity_sub_2] = list_ids[y:]
            
            for rid in list_ids[:y]:
                dict_id_2_identity_split[rid] = identity_sub_1
            for rid in list_ids[y:]:
                dict_id_2_identity_split[rid] = identity_sub_2

        else:
            dict_identity_2_list_ids_split[identity] = list_ids
            for rid in list_ids:
                dict_id_2_identity_split[rid] = identity
    return dict_identity_2_list_ids_split, dict_id_2_identity_split






def bootstrap_stageII():
    '''
    Bootstrap the stage II identity linking by splitting
    stage I identities randomly and observing the relationship
    between identity field comparison scores and matching
    likelihood.
    '''
    import cPickle

    # Whether to split the identities afresh (!)
    split_fresh = True

    # Whether to rescan for candidate record pairs.
    # This must be recomputed every time a new idm_split
    # is generated.
    find_candidate_pairs = True

    # Whether to recompute and partition 
    # identity pairs to be compared.
    partition_fresh = True 



    num_partitions = 24

    # Number of candidate record pairs to find.
    num_pairs = 50000000
    
    if True:
        if split_fresh:
            idm = Database.IdentityManager('USA')
            idm.fetch_dict_id_2_identity()
            idm.fetch_dict_identity_2_id()

            # The split version of dict_identity_2_list_ids
            dict_identity, dict_id = split_dict_identity_ids(idm.dict_identity_2_list_ids,0.5)
            with open('dict_identity_v{version}.pickle'.format(version = config.FEC_version_disambiguation),'w') as f:
                cPickle.dump(dict_identity, f)
            with open('dict_identity_v{version}.pickle'.format(version = config.FEC_version_disambiguation),'w') as f:
                cPickle.dump(dict_id, f)
        else:
            with open('dict_identity_v{version}.pickle'.format(version = config.FEC_version_disambiguation)) as f:
                dict_identity = cPickle.load(f)
            with open('dict_identity_v{version}.pickle'.format(version = config.FEC_version_disambiguation)) as f:
                dict_id = cPickle.load(f)
             
        # An idm loaded with the split dictionaries.
        idm_split = Database.IdentityManager('USA')
        idm_split.dict_identity_2_list_ids = dict_identity
        idm_split.dict_id_2_identity = dict_id

        if find_candidate_pairs:
            get_candidate_pairs(num_pairs, recompute = True, idm = idm_split, mode = "bootstrapping")
        
        if split_fresh or partition_fresh:
            partition_S1_identities(num_partitions = num_partitions, state = 'USA', idm = idm_split, mode = "bootstrapping")

        # Run a bootstrapping 
        disambiguate_subsets_multiproc(num_partitions=num_partitions, state="USA", num_procs=10, idm = idm_split, mode = "bootstrapping") 

    # now analyze the bootstraping results and 
    # export data files to be used for defining
    # verdict authority.
    process_bootstrapping_results()
    


def __get_split_matches():
    '''
    Load the set of identity matches recovered via
    the bootstrapping process.
    '''
    import re
    set_identity = set()
    split_matches = []
    non_split_matches = []
    with open(config.S2_bootstrap_results_file) as f:
        counter = 0
        counter_line = 0
        counter_both = 0
        counter_non_match = 0
        for line in f:
            counter_line += 1
            data = utils.json.loads(line)
            

            match1 = re.findall(r'(.*)\|([12])', data[0])
            match2 = re.findall(r'(.*)\|([12])', data[1])
            if match1:
                set_identity.add(data[0])
            elif match2:
                set_identity.add(data[1])
            
            if not match1 or not match2:
                non_split_matches.append(data[2:])
                counter_non_match += 1
            
            if match1 and match2 :
                counter_both += 1
                if match1[0][0] == match2[0][0]:
                    counter += 1
                    split_matches.append(data[2:])
    return split_matches, non_split_matches
            

def __cum_threshold(myarray, percentile):
    '''
    For an array (possibly 2d), return the threshold
    value corresponding to the top percentile of the
    cumulative sum of the sorted array.
    In other words, the return value C{y} is such that
    all entries in C{myarray} with values above c{y} will
    add up to c{percentile} percent of the total sum of
    C{myarray}'s entries.
    '''
    freqs_sorted = sorted(myarray.flatten())
    x = np.cumsum(freqs_sorted)
    xmax = max(x)

    # index for 95 percentile of cumsum
    cutoff_index = np.where(x > xmax*(1-percentile/100.))[0][0]
    return freqs_sorted[cutoff_index]
    

def __heatmap(xx, yy, title, subplot,cum_percent = 95, aspect = 1, xlim = None, ylim = None):
    '''
    This method takes the coordinates for a bunch of points,
    calculates a 2D histogram of them, then finds the region
    in the parameter space that contains a certain percentage
    of the total point frequency. Then, the histogram is plotted
    with the region delineated by a contour line. Finally, the
    function returns a list of coordinate tuples that fall in
    the region. 
    '''
    from scipy import ndimage
    import matplotlib.pyplot as plt
    import matplotlib

    contour_colors = 'w'
    bins = [np.arange(0,1200)-0.5, np.arange(0,100)-0.5]

    plt.subplot(3,2,subplot)
    indices = np.where(~np.isnan(xx) & ~np.isnan(yy))[0]
    freqs, xe, ye = np.histogram2d( xx[indices], yy[indices], bins, normed = False)
    range1 = [0,max(ye), max(xe), 0]

    threshold = __cum_threshold(freqs, cum_percent)
    filtered_indices = np.where(freqs > threshold)
    freqs_tmp = freqs + 1.0-1.0
    np.place(freqs_tmp, freqs == 0, 1)

    a = np.log10(freqs_tmp)
    plt.imshow(a, cmap='hot', interpolation='none', extent = range1 )
    formatter = matplotlib.ticker.LogFormatter(10, labelOnlyBase=False) 

    #colorbar = plt.colorbar(format = formatter)
    #colorbar = plt.colorbar()

    print threshold
    plt.gca().invert_yaxis()
    
    freqs = ndimage.gaussian_filter(freqs, sigma=(1.5, 1.5), order=0)
    plt.contour(np.flipud(freqs), colors=contour_colors, levels = [threshold],\
                extent = range1, antialiased = True, nchunk = 100 )
    inds = np.where(freqs > threshold)
    filter_points =  [(inds[1][i], inds[0][i]) for i in range(len(inds[0]))]
#     plt.gca().axis('tight') 
    plt.xlabel('Freq with middle name')
    plt.ylabel('Freq without middle name')
    plt.title(title)
    plt.gca().set_aspect(aspect)
    plt.xlim(xlim)
    plt.ylim(ylim)
    
    #plt.savefig('bootstrapping-n_4-%s.png' % title.replace('\n','').replace(' ','_'))
    
    return filter_points




def __threshold_freqs(xx, subplot, cum_percent = 95, title = ''):
    '''
    Compute and export to file the name frequency thresholds
    for various affiliation match scores in the case where
    names match but not on the middle name.
    '''
    import matplotlib.pyplot as plt
    def get_1d_threshold(freqs, cum_percent):
        '''
        Given a descending list of frequencies, compute the
        index of the entry x such that the cumulative sum of
        all entries up to x is C{cum_percent} percent of the
        sum of all entries.
        '''

        cumsums = np.cumsum(freqs)
        total = cumsums[-1]
        threshold = total * (cum_percent / 100.0)
        return np.where(cumsums > threshold)[0][0]

    plt.subplot(2,3,subplot)
    
    bins = np.arange(0,1200)-0.5
    indices = np.where(~np.isnan(xx))[0]
    # plt.hist(xx[indices],bins = bins)
    freqs, xs = np.histogram(xx[indices],bins = bins)
    # freqs = freqs[:-1]
    centers = ((xs[:-1] + xs[1:])/2).astype(int)
    plt.loglog(centers,freqs,'.k')
    threshold = get_1d_threshold(freqs, cum_percent)
    t = threshold
    print t
    plt.loglog([t,t] , [1, 1e5],'r')
    plt.ylim((1,1e5))
    plt.xlabel('name frequency')
    plt.gca().set_aspect(0.6)
    plt.title(title)
    return int(threshold)
        
def process_bootstrapping_results():
    import matplotlib.pyplot as plt

    split_matches, non_split_matches = __get_split_matches()


    filter_occupation_2 = lambda x: (x[1][0] == 2) # occupation
    filter_employer_2 = lambda x: (x[2][0] == 2) # employer

    filter_occupation_3 = lambda x: (x[1][0] == 3) # occupation
    filter_employer_3 = lambda x: (x[2][0] == 3) # employer

    filter_occupation_4 = lambda x: (x[1][0] == 4) # occupation
    filter_employer_4 = lambda x: (x[2][0] == 4) # employer

    filter_name_3 = lambda x: (x[0][0] == 3) # name = 3
    filter_name_4 = lambda x: (x[0][0] == 4) # name = 3


    # frequencies if occupations are linked
    f_n_4_o_2_middle = np.asarray([x[0][1][0] if filter_occupation_2(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_4_o_2 = np.asarray([x[0][1][1] if filter_occupation_2(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_3_o_2 = np.asarray([x[0][1][1] if filter_occupation_2(x) and filter_name_3(x) else None for x in split_matches ], dtype=np.float)


    # frequencies if occupations are linked
    f_n_4_o_3_middle = np.asarray([x[0][1][0] if filter_occupation_3(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_4_o_3 = np.asarray([x[0][1][1] if filter_occupation_3(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_3_o_3 = np.asarray([x[0][1][1] if filter_occupation_3(x) and filter_name_3(x) else None for x in split_matches ], dtype=np.float)


    # frequencies if occupations are identical
    f_n_4_o_4_middle = np.asarray([x[0][1][0] if filter_occupation_4(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_4_o_4 = np.asarray([x[0][1][1] if filter_occupation_4(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_3_o_4 = np.asarray([x[0][1][1] if filter_occupation_4(x) and filter_name_3(x) else None for x in split_matches ], dtype=np.float)






    # frequencies if employers are linked
    f_n_4_e_2_middle = np.asarray([x[0][1][0] if filter_employer_2(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_4_e_2 = np.asarray([x[0][1][1] if filter_employer_2(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_3_e_2 = np.asarray([x[0][1][1] if filter_employer_2(x) and filter_name_3(x) else None for x in split_matches ], dtype=np.float)

    # frequencies if employers are linked
    f_n_4_e_3_middle = np.asarray([x[0][1][0] if filter_employer_3(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_4_e_3 = np.asarray([x[0][1][1] if filter_employer_3(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_3_e_3 = np.asarray([x[0][1][1] if filter_employer_3(x) and filter_name_3(x) else None for x in split_matches ], dtype=np.float)

    # frequencies if employers are identical
    f_n_4_e_4_middle = np.asarray([x[0][1][0] if filter_employer_4(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_4_e_4 = np.asarray([x[0][1][1] if filter_employer_4(x) and filter_name_4(x) else None for x in split_matches ], dtype=np.float)
    f_n_3_e_4 = np.asarray([x[0][1][1] if filter_employer_4(x) and filter_name_3(x) else None for x in split_matches ], dtype=np.float)



    #######################################################
    # NAMES MATCH INCLUDING ON MIDDLE NAME      
    #######################################################
    plt.figure(figsize=(10,15))

    # Get a list of tuples, each one acceptable point coordinate
    inds_n_4_o_4 = __heatmap(f_n_4_o_4, f_n_4_o_4_middle, 'Log-histogram of matches with \n identical occupations', 1,\
            aspect = 0.05, xlim=(0,20), ylim=(0,400))
    inds_n_4_e_4 = __heatmap(f_n_4_e_4, f_n_4_e_4_middle, 'Log-histogram of matches with \n identical employers', 2,\
            aspect = 0.05, xlim=(0,20), ylim=(0,400))
    inds_n_4_o_3 = __heatmap(f_n_4_o_3, f_n_4_o_3_middle, 'Log-histogram of matches with \n LINKED occupations', 3,\
            aspect = 0.2, xlim=(0,15), ylim=(0,100))
    inds_n_4_e_3 = __heatmap(f_n_4_e_3, f_n_4_e_3_middle, 'Log-histogram of matches with \n LINKED employers', 4,\
            aspect = 0.2, xlim=(0,15), ylim=(0,100))

    inds_n_4_o_2 = __heatmap(f_n_4_o_2, f_n_4_o_2_middle, 'Log-histogram of matches with \n BAD occupations', 5,\
            aspect = 0.2, xlim=(0,15), ylim=(0,100))
    inds_n_4_e_2 = __heatmap(f_n_4_e_2, f_n_4_e_2_middle, 'Log-histogram of matches with \n BAD employers', 6,\
            aspect = 0.2, xlim=(0,15), ylim=(0,100))
    
    plt.savefig('bootstrapping-n_4.png')


    #######################################################
    # NAMES MATCH BUT NOT ON MIDDLE NAME      
    #######################################################

    plt.figure(figsize=(10,7))

    # dict of name frequency thresholds for the case
    # when there is no middle name match.
    thresholds = {}
    thresholds['README'] = "This dictionary contains stage II 95 percentile name frequency thresholds \
    for various affiliation match scores in the case where names \
    match but not on the middle name. e_2 means the case where employers \
    have a match score of 2. o_2 is the same for occupations, etc."
    thresholds['e_2'] = __threshold_freqs(f_n_3_e_2, 1, title = 'BAD employers')
    thresholds['e_3'] = __threshold_freqs(f_n_3_e_3, 2, title = 'Linked employers')
    thresholds['e_4'] = __threshold_freqs(f_n_3_e_4, 3, title = 'Identical employers')

    thresholds['o_2'] = __threshold_freqs(f_n_3_o_2, 4, title = 'BAD occupations')
    thresholds['o_3'] = __threshold_freqs(f_n_3_o_3, 5, title = 'Linked occupations')
    thresholds['o_4'] = __threshold_freqs(f_n_3_o_4, 6, title = 'Identical occupations')
    plt.savefig('bootstrapping-n_3.png')




    filename_inds_n_4_o_4 = config.filename_inds_n_4_o_4
    filename_inds_n_4_e_4 = config.filename_inds_n_4_e_4
    filename_inds_n_4_o_3 = config.filename_inds_n_4_o_3
    filename_inds_n_4_e_3 = config.filename_inds_n_4_e_3
    filename_inds_n_4_o_2 = config.filename_inds_n_4_o_2
    filename_inds_n_4_e_2 = config.filename_inds_n_4_e_2

    filename_inds_n_3 = config.filename_inds_n_3
    
    try:
        with open(filename_inds_n_4_o_4, 'w') as f:
            utils.json.dump(inds_n_4_o_4, f)

        with open(filename_inds_n_4_e_4, 'w') as f:
            utils.json.dump(inds_n_4_e_4, f)

        with open(filename_inds_n_4_o_3, 'w') as f:
            utils.json.dump(inds_n_4_o_3, f)

        with open(filename_inds_n_4_e_3, 'w') as f:
            utils.json.dump(inds_n_4_e_3, f)

        with open(filename_inds_n_4_o_2, 'w') as f:
            utils.json.dump(inds_n_4_o_2, f)

        with open(filename_inds_n_3, 'w') as f:
            utils.json.dump(thresholds, f)


    except Exception as e:
        print "ERROR: unable to export ind files."
        raise e






if __name__ == "__main__":

    
    #bootstrap_stageII()
    #partition_S1_identities(24, state = "USA", idm = None, mode="disambiguation")
    quit()

    #process_bootstrapping_results()









############################################################################################
# DEPRECATED METHODS
############################################################################################



def __get_customized_verdict_OLD(verdict, detailed_comparison_result):
    '''
    @deprecated: used for v1 and v2. 
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

    As an exmple, the fullname frequencies can be used to compute a positive
    score if C{verdict == True}.
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
        - B{>0}: records are a match.
        - B{None}: records are't similar, just ignore them.
    '''
    print "Running D.compare_list_of_pairs(list_of_pairs)"
    # D.compare_list_of_pairs is a generator: it yields the full results of
    # the pairwise comparisons and we are able to perform one last analysis and
    # decide what whether the math is a "no", "maybe" or "yes".
    for verdict, detailed_comparison_result, rid1, rid2 in  D.compare_list_of_pairs(list_of_pairs):
        final_result = __get_customized_verdict_OLD(verdict, detailed_comparison_result)
        list_record_pairs.append(((rid1, rid2), final_result))

    return list_record_pairs



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
np = utils.np
import sys, traceback



def get_candidate_pairs(num_pairs, state='USA', recompute=False, idm = None, mode='disambiguation'):
    '''
    Get pairs of record ids that are similar according
    to the national (combined) hashes, but aren't already
    linked at the state level.
    @param state: the state whose hashes will be used.
    @param num_pairs: number of new records to select for comparison
    @param idm: L{IdentityManager} instance to use. This is used to that
    records pairs both from the same identity are simply skipped.
    @param mode: can be 'disambiguation' or "bootstrapping". The results are
    written to different files depending on the value of this parameter.
    @return: list of tuples of record ids.
    '''

    # file that either already contains, or will contain a set of candidate
    # pairs of record ids together with a "weight"
    if mode == "disambiguation":
        #candidate_pairs_file = config.candidate_pairs_file_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_template
    elif mode == "bootstrapping":
        #candidate_pairs_file = config.candidate_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_file = config.candidate_S1_identity_pairs_file_bootstrapping_template % state
        candidate_S1_identity_pairs_partitioned_file_template = config.candidate_S1_identity_pairs_partitioned_file_bootstrapping_template 
        candidate_list_records_partitioned_file_template = config.candidate_list_records_partitioned_file_bootstrapping_template
    else:
        raise ValueError("ERROR: parameter 'mode' must be either 'disambiguation' or 'bootstrapping'.")
    
        

    # Make sure the file already exists
    if not recompute:
        if utils.os.path.exists(candidate_pairs_file):
            print "Candidate pairs file already exists. Skipping."
            return

    # Number of adjacent hashes to log
    B = 15

    # number of times to shuffle the list of hashes.
    num_shuffles = 40

    # list of candidate pairs to be compared.
    list_pairs = []

    # Get the full sorted edgelist from national hashes.
    filename = config.hashes_file_template % (state, 'Tokenizer')

    print "Starting get_edgelist_from_hashes_file()..."

    # NOTE: There is a particular case where relying on record edgelist fails us: consider the case
    # where there is one cluster with a single record and another with 100 records. Suppose that single
    # record is similar to quite a few of the records in the larger cluster. Using the hashing method
    # as we've done before will have the following result: the single record will definitely be found
    # close to *some* record from the large cluster in different permutations. Hoever, it may not end
    # up close to any one of them sonsistently enough that their proximity will be registered. 
    # So, instead of recording which records are proximare, we should register which *identities* are
    # proximate via their records.
    # NOTE: implement the new version.
    edgelist = hashes.get_edgelist_from_hashes_file(filename, state, B, num_shuffles, num_procs=3, num_pairs=num_pairs, idm = idm, prune = True)
    # edgelist.reverse()
    print "Done with get_edgelist_from_hashes_file()"
    print "Done fetching new pairs."

    # NOTE: new version produces edgelist of identities
    # Instead of records.
    # Export identity edgelist to file
    with open(candidate_S1_identity_pairs_file, 'w') as f:
        for edge in edgelist:
            #f.write("%s %s %d\n" % (edge[0], edge[1], edge[2]))
            f.write("%s %s\n" % (edge[0], edge[1]))




def partition_records(num_partitions, state="USA"):
    '''
    @deprecated: 
    Partition the set of records appearing in the pairs identified
    by L{get_candidate_pairs()} into num_partitions subsets
    with minimal inter-set links, and export the edgelists within
    each sibset to a separate file for each subset. The list of record
    pairs is read from a file generated by L{get_candidate_pairs()}.

    @param num_partitions: number of partitions to divide C{list_record_pairs} into.
    @status: only works if the giant component isn't "too" large. Otherwise,
    the partitions aren't balanced.
        print "Running D.compare_list_of_pairs(list_of_pairs)"
        # D.compare_list_of_pairs is a generator: it yields the full results of
        # the pairwise comparisons and we are able to perform one last analysis and
        # decide what whether the math is a "no", "maybe" or "yes".
        for verdict, detailed_comparison_result, rid1, rid2 in  D.compare_list_of_pairs(list_of_pairs):
            final_result = __get_customized_verdict_OLD(verdict, detailed_comparison_result)
            list_record_pairs.append(((rid1, rid2), final_result))

        return list_record_pairs
    '''
    pass


