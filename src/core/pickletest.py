import sys
import copy
from math import ceil
from cPickle import dumps
import numpy as np
import random

def chunkit_padded(list_input, i, num_chunks, overlap=0):
    '''
    This function splits a list into "total_count" sublists of roughly equal sizes and returns the "ith" one.
    These lists are overlapping.
    '''
    n = len(list_input)

    size = float(n) / num_chunks

    x, y = int(ceil(i * size)), min( int(ceil((i + 1) * size)) + overlap, n)
    return list_input[x:y]


def random_uniform_hyperspherical(n):
    vec = np.zeros([n, ])
    for i in range(n):
        vec[i] = random.gauss(0, 1)
    vec = vec / np.linalg.norm(vec)
    return vec



num_procs = 10

list_probe_vectors = [random_uniform_hyperspherical(40000) for i in range(40)]

list_feature_vecs = [{i:i+1 for i in range(1000)} for j in range(10000)]
#list_feature_vecs = [r.vector for r in self.list_of_records]
list_data1 = [{'id': i,
              'vecs':  chunkit_padded(list_feature_vecs, i, num_procs, overlap=0),
              'hash_dim':40,
              'probe_vectors':list_probe_vectors} for i in range(num_procs)]


list_data2 = [{'id': i,
              'vecs':  chunkit_padded(list_feature_vecs, i, 1, overlap=0),
              'hash_dim':40,
              'probe_vectors':list_probe_vectors} for i in range(1)]


# 2
#memory_spike(2)


#list_data1 = [self.list_of_records] + ['' for i in range(num_procs-1)]  #320MB
#list_data2 = [chunkit_padded(self.list_of_records, i, num_procs, overlap=0) for i in range(num_procs)]   #570MB
#results = pool.map(get_hashes, list_data)

print sum([sys.getsizeof(dumps(d)) for d in list_data1])
print sum([sys.getsizeof(dumps(d)) for d in list_data2])
print sys.getsizeof(dumps(list_probe_vectors))
quit()
