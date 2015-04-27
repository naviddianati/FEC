import re
import random
import resource
import numpy as np

"""
this file contains generic functions used by various scripts
"""



def bad_identifier(identifier, type='employer'):    
    if identifier == '': return True
    if type == 'employer':
        regex = r'\bNA\b|N\.A|employ|self|N\/A|\
                |information request|retired|teacher\b|scientist\b|\
                |applicable|not employed|none|\
                |homemaker|requested|executive|educator\b|\
                |attorney\b|physician|real estate|\
                |student\b|unemployed|professor|refused|doctor|housewife|\
                |at home|president|best effort|consultant\b|\
                |email sent|letter sent|software engineer|CEO|founder|lawyer\b|\
                |instructor\b|chairman\b'
    elif type == 'occupation':
        regex = r'unknown|requested|retired|none|retire|retited|ret\b|declined|N.A\b|refused|NA\b|employed|self'
    else:
        print 'invalid identifier type'
        quit()
#     print identifier
    if re.search(regex, identifier, flags=re.IGNORECASE): 
        return True
    else: 
        return False






def argsort(seq):
    ''' Generic argsort. returns the indices of the sorted sequence'''
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)

def shuffle_str(s):
    ''' Shuffle string by converting to list, shuffling and converting back '''
    l = list(s)
    random.shuffle(l)
    return ''.join(l)


def sparse_vector(vec):
    ''' This function converts a numpy 2d vector to a dictionary
        where the key is the index of the vector element and the
        value is the vector component corresponding to that coordinate '''
    vec_sparse = {}
    for i in range(len(vec)):
        if vec[i] != 0:
            vec_sparse[i] = vec[i]
    return vec_sparse

def vec_norm(vec):
    ''' this function computes the 2-norm of a sparse vector.'''
    total = 0
    for ind in vec:
        total += vec[ind] ** 2
    return np.sqrt(total)

def inner_product(vec_short, vec):
    ''' This function computes the inner product of two sparse vectors.
        It is assumed that the first argument is the shorter of the two.'''
    ''' The inned_product function takes advantage of the sparse representation of the vectors
        to significantly optimize the operation. The complexity of the inner product is independent
        of dim, it only depends on the average number of "non-zero" elements in the vectors. ''' 
    total = 0
#     vec_keys = vec.keys() # SO EXPENSIVE!!!!
    counter = 0
    for ind in vec_short:
        counter += 1
#         if ind in vec: 
#         print ind,vec_short,vec
        total += vec_short[ind] * vec[ind]
#     print counter
    return total 
    


    
# number of vectors

def generate_rand_list_of_vectors(N, dim):
    # Generate a random set of input vectors
    list_of_vectors = []
    i = 0
    cluster_size = 0
    vec_previous = []
    while (i < N):
        vec = {}
        if cluster_size == 0:
            cluster_size = round(abs(random.gauss(0, 1) * 5)) + 1
        else: 
            cluster_size -= 1
        
        # generate a random sparse vector 
        if cluster_size != 0:
            for j in vec_previous:
                if random.random() < 0.3:
                    vec[j] = 1
        for j in range(25):
            if random.random() < 0.05:
                vec[random.randint(0, dim - 1)] = 1
        if vec:
            list_of_vectors.append(vec)
            vec_previous = vec
            i += 1
    return list_of_vectors




def print_resource_usage(msg):
    print msg, resource.getrusage(resource.RUSAGE_SELF)


