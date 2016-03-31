'''
This module contains the worker function that computes
the LSH hashes for the records. It is called by Disambiguator
instances.
'''
import time
import utils
import Database

def get_edgelist_from_hashes(filename, state, B=10, num_shuffles=10, idm = None):
    '''
    Shuffle the given  hashes num_shuffles times and build
    an edgelist of identities corresponding to item pairs most 
    commonly found close to one another in the sorted hash list.

    @param filename: filename of the hashes.
    @param B: total number of adjacenct hashes to log for each sorting
        of the list of hashes.
    @param num_shuffles: number of times to shuffle the hashes and log the neighbors.
    @param idm: L{IdentityManager} instance to use

    OLD: return: a dictionary {(r_id1, r_id2): score}.
    @return: a dictionary {(identity1, identity2): score}.
    '''
    print "Loading the hash file..."
    with open(filename) as f:
        dict_hashes = utils.cPickle.load(f)
    print "Done loading the hash file."

    # instantiate an IdentityManager instace for this process
    if not idm:
        idm = Database.IdentityManager(state=state)
        idm.fetch_dict_id_2_identity()
    dict_id_2_identity = idm.dict_id_2_identity

    ids = []
    ss = []
    for rid in dict_hashes.keys():
        s = dict_hashes.pop(rid)
        ids.append(rid)
        ss.append(s)
    del dict_hashes
    print "Done initializing lists of ids and hashes."
    # ids = dict_hashes.keys()
    # ss = dict_hashes.values()
    n = len(ss)
    utils.random.seed()
    for counter in range(num_shuffles):
        edgelist = set()

        utils.shuffle_list_of_str(ss)
        print "Done shuffling ss."
        arg = utils.np.argsort(ss, axis=0, kind='heapsort')
        print "Done sorting."

        counter_linked = 0
        counter_new = 0
        counter_not_found = 0

        for i, s in enumerate(arg):
            # j_low , j_high = max(0, i - B / 2), min(i + B / 2, n - 1)
            j_low , j_high = i, min(i + B, n - 1)
            for j in range(j_low, j_high + 1):
                x = ids[arg[i]]
                y = ids[arg[j]]
                if i == j: continue
                try:
                    identity1, identity2 = dict_id_2_identity[x], dict_id_2_identity[y]
                    identity1 = min(identity1, identity2)
                    identity2 = max(identity1, identity2)
                except:
                    counter_not_found += 1
                    continue
                if identity1 == identity2:
                    counter_linked += 1
                    continue

                counter_new += 1
                # NOTE: now, instead of the record ids, we 
                # register their identities.
                edgelist.add((identity1, identity2))

                # This was back when instead of edgelist I had an adj:
                # try:
                #    #adj[(xmin, xmax)] += np.exp(-(np.abs(i - j)))
                #    edgelist[(xmin, xmax)] += 1
                # except:
                #    #adj[(xmin, xmax)] = np.exp(-(np.abs(i - j)))
                #    edgelist[(xmin, xmax)] = 1
        print "shuffle done. %d -- %d" % (counter, len(edgelist))
        print "pairs already linked: %d" % counter_linked
        print "pairs NOT already linked: %d" % counter_new
        print "pairs where one id has no identity: %d" % counter_not_found
        yield edgelist
        del edgelist




def get_edgelist_from_hashes_file(filename, state, B=10, num_shuffles=40, num_procs=12, num_pairs=1000, idm = None, prune = True):
    '''
    Using multiple child processes, load hashes, shuffle them multiple
    times and compute identity edgelist. Worker function: L{worker_get_edgelist_from_hashes_file}

    @param filename: filename where hashes are pickled.
    @param state: the state.
    @param B: number of adjacency hashes to log.
    @param num_shuffles: total number of times to shuffle the hashes.
    @param num_procs:  number of processes to use.
    @param idm: L{IdentityManager} instance to use.
    @param prune: whether to prune the adjacency list after every 10
    shuffles. 
    '''

    # The full adjacency matrix. A dict.
    adj_full = {}


    # Queue to be shared between processes
    q = utils.multiprocessing.Queue(maxsize=3)

    # Compute each process's share of num_shuffles.
    list_num_shuffles = utils.partition_integer(num_shuffles, num_procs)

    # Each child process receives the filename, B, and the integer
    # number of times it is supposed to shuffle the hashes. Plus the queue.
    data = [(filename, state, B, n, q, idm) for n in list_num_shuffles]

    list_procs = []

    for pid in range(num_procs):
        p = utils.multiprocessing.Process(target=worker_get_edgelist_from_hashes_file
                                    , args=(data[pid],))
        p.start()
        print "process %d started..." % pid
        list_procs.append(p)

    # Receive results as they are produced and
    # combine them on the fly.
    results_counter = 0
    while results_counter < num_shuffles:
        # utils.time.sleep(0.5)
        # if q.empty(): continue

        # After receiving each 10 results, prune adj_full
        # by removing keys with values == 1. i.e., if a pair
        # of records haven't been adjacent after 10 reshuffles,
        # then they are hopeless, dead weight.
        if results_counter % 10 == 9:
            if prune:
                print "--------- Pruning adj_full..."
                adj_full = utils.prune_dict(adj_full, lambda x: (x > 1))
        edgelist_new = q.get()
        print "received results..."
        results_counter += 1

        # update adj_full
        for id_tuple in edgelist_new:
            try:
                adj_full[id_tuple] += int(1)
            except KeyError:
                adj_full[id_tuple] = int(1)
        edgelist_new.clear()
        print "Done processing result no %d" % results_counter
        print "Total size of adj_full: %d" % len(adj_full)

    # prune adj_full one last time.
    adj_full = utils.prune_dict(adj_full, lambda x: (x > 1))
    # Convert adj_full to a list of tuples
    edgelist = [(x[0], x[1], y) for x, y in adj_full.iteritems()]

    del adj_full
    edgelist.sort(key=lambda x: x[2], reverse=True)
    del edgelist[num_pairs:]

    return edgelist



def worker_get_edgelist_from_hashes_file(data):
    '''
    Retrieve list of hashes from filename, and get
    an edgelist of identity pairs deemed close according to
    the hashes.
    '''
    # Unpack init data.
    filename, state, B , num_shuffles, queue, idm = data
    pid = utils.multiprocessing.current_process().name
    for edgelist in  get_edgelist_from_hashes(filename, state, B, num_shuffles, idm):
        print "Sending data to queue on process %s len(adj): %d" % (pid, len(edgelist))

        queue.put(edgelist)
        print "Done sending data to queue on process %s " % pid





def worker_compute_hashes(data):
    '''
    Receive a chunk of the record vectors and compute their LSH hashes given
    a list of probe vectors.

    data is a dict.
    data['id']: which chunk of data is being passed to this worker, integer
    data['vecs']: a set of record feature vectors
    data['hash_dim']: hash_dim
    data['probe_vecs']: a list of hash_dim (all) probe vectors
    '''


    dict_vectors = data[0]['vecs']
    hash_dim = data[0]['hash_dim']
    probe_vectors = data[1]
    pid = data[0]['id']
    print "Generating hashes in process %d" % pid
    # Number of records being processed by this worker
    n = len(dict_vectors)

#     LSH_hash = ['' for i in xrange(n)]
    dict_hashes = {int(r_id):'' for r_id in dict_vectors}
    print "length of probe_vectors:", len(probe_vectors)

    for k in range(hash_dim):
        # random "probe" vector
        vec_tmp = probe_vectors[k]

        # convert to sparse form
        vec = utils.sparse_vector(vec_tmp)
        vec_n = utils.vec_norm(vec)
        # for record, i in zip(self.list_of_records, range(N)):
        for r_id, v in dict_vectors.iteritems():
            c = '1' if utils.inner_product(v, vec) > 0 else '0'
            dict_hashes[int(r_id)] += c
    return (pid, dict_hashes)


if __name__ == "__main__":
    pass
