'''
This module contains the worker function that computes
the LSH hashes for the records. It is called by Disambiguator
instances.
'''
from common import *
import time
# from utils import *
import utils


def get_edgelist_from_hashes(dict_hashes, B=10, num_shuffles=10):
    '''
    Shuffle the given  hashes num_shuffles times and build
    an edgelist of item pairs most commonly found close
    to one another in the sorted hash list.

    @param dict_hashes: a dict {r_id: hash}.
    @param B: total number of adjacenct hashes to log for each sorting
        of the list of hashes.
    @param num_shuffles: number of times to shuffle the hashes and log the neighbors.

    @return: a dictionary {(r_id1, r_id2): score}.
    '''
    ids = dict_hashes.keys()
    ss = dict_hashes.values()
    n = len(ss)
    adj = {}
    random.seed()
    for counter in range(num_shuffles):
        utils.shuffle_list_of_str(ss)
        arg = np.argsort(ss, axis=0)
        for i, s in enumerate(arg):
            j_low , j_high = max(0, i - B / 2), min(i + B / 2, n - 1)
            for j in range(j_low, j_high + 1):
                x = ids[arg[i]]
                y = ids[arg[j]]
                if i == j: continue
                xmin = min(x, y)
                xmax = max(x, y)
                try:
                    adj[(xmin, xmax)] += np.exp(-(np.abs(i - j)))
                except:
                    adj[(xmin, xmax)] = np.exp(-(np.abs(i - j)))
        print "%d -- %d" % (counter, len(adj))
    return adj
    pass



def get_edgelist_from_hashes_file(filename, B=10, num_shuffles=40, num_procs=12):
    '''
    Using multiple child processes, load hashes, shuffle them multiple
    times and compute edgelist. Worker function: worker_get_edgelist_from_hashes_file()

    @param filename: filename where hashes are pickled. 
    @param B: number of adjacency hashes to log.
    @parma num_shuffles: total number of times to shuffle the hashes.
    @param nump_procs:  number of processes to use.
    '''

    # The full adjacency matrix. A dict.
    adj_full = {}


    # Queue to be shared between processes
    q = utils.multiprocessing.Queue()

    # Compute each process's share of num_shuffles.
    list_num_shuffles = utils.partition_integer(num_shuffles, num_procs)

    # Each child process receives the filename, B, and the integer
    # number of times it is supposed to shuffle the hashes. Plus the queue.
    data = [(filename, B, n, q) for n in list_num_shuffles]

    list_procs = []

    for pid in range(num_procs):
        p = utils.multiprocessing.Process(target=worker_get_edgelist_from_hashes_file
                                    , args=(data[pid],))
        p.start()
        list_procs.append(p)

    # Receive results as they are produced and
    # combine them on the fly.
    results_counter = 0
    while results_counter < num_procs:
        utils.time.sleep(0.5)
        if q.empty(): continue
        print "received results..."
        adj_new = q.get()
        results_counter += 1

        # update adj_full
        for id_tuple, weight in adj_new.iteritems():
            try:
                adj_full[id_tuple] += weight
            except KeyError:
                adj_full[id_tuple] = weight
        print "Done processing result."
    # Convert adj_full to a list of tuples
    edgelist = [(x[0], x[1], y) for x, y in adj_full.iteritems()]
    del adj_full
    edgelist.sort(key=lambda x: x[2], reverse=True)

    return edgelist



def worker_get_edgelist_from_hashes_file(data):
    '''
    Retrieve list of hashes from filename, and get
    an edgelist of pairs deemed close according to
    the hashes.
    '''
    # Unpack init data.
    filename, B , num_shuffles, queue = data

    f = open(filename)
    dict_hashes = utils.cPickle.load(f)
    f.close()
    adj = get_edgelist_from_hashes(dict_hashes, B, num_shuffles)
    queue.put(adj)





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
    dict_hashes = {r_id:'' for r_id in dict_vectors}
    print "length of probe_vectors:", len(probe_vectors)

    for k in range(hash_dim):
        # random "probe" vector
        vec_tmp = probe_vectors[k]

        # convert to sparse form
        vec = sparse_vector(vec_tmp)
        vec_n = vec_norm(vec)
        # for record, i in zip(self.list_of_records, range(N)):
        for r_id, v in dict_vectors.iteritems():
            c = '1' if inner_product(v, vec) > 0 else '0'
            dict_hashes[r_id] += c
    return (pid, dict_hashes)


if __name__ == "__main__":
    pass
