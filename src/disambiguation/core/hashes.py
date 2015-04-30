'''
This module contains the worker function that computes
the LSH hashes for the records. It is called by Disambiguator
instances.
'''
from common import *
import time
from utils import *


def get_edgelist_from_hashes(dict_hashes, B=10, num_shuffles=10):
    '''
    Shuffle the given  hashes num_shuffles times and build
    an edgelist of item pairs most commonly found close
    to one another in the sorted hash list.

    @param dict_hashes: a dict {r_id: hash}.
    @param B: total number of adjacenct hashes to log for each sorting
        of the list of hashes.
    @param num_shuffles: number of times to shuffle the hashes and log the neighbors.

    @return: a list of tuples (r_id1, r_id2, score).
    '''
    ids = dict_hashes.keys()
    ss = dict_hashes.values()
    n = len(ss)
    adj = {}
    random.seed()
    for counter in range(num_shuffles):
        shuffle_list_of_str(ss)
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
        print len(adj)
    edgelist = [(x[0], x[1], y) for x, y in adj.iteritems()]
    return edgelist
    pass






def get_edgelist_from_hashes_file(filename, B=10, num_shuffles=10):
    '''
    Retrieve list of hashes from filename, and get
    an edgelist of pairs deemed close according to
    the hashes.
    ''' 
    f = open(filename)
    dict_hashes = cPickle.load(f)
    return get_edgelist_from_hashes(dict_hashes, B, num_shuffles), dict_hashes





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
