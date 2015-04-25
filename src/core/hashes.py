'''
This module contains the worker function that computes
the LSH hashes for the records. It is called by Disambiguator
instances.
'''
from common import *
import time

def get_hashes(data):
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
        #for record, i in zip(self.list_of_records, range(N)):
        for r_id, v in dict_vectors.iteritems():
            c = '1' if inner_product(v, vec) > 0 else '0'
            dict_hashes[r_id] += c
    return (pid,dict_hashes)


if __name__ == "__main__":
    pass
