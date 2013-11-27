import random
import pprint
import numpy as np
import pylab as pl
# import visual as vs 
import time
import os
import editdist
import json


class Disambiguator():
    def __init__(self, list_of_vectors, index_2_token, token_2_index, input_dimensions, batch_id):
        self.batch_id = batch_id
        self.index_2_token = index_2_token
        self.token_2_index = token_2_index
        self.list_of_vectors = list_of_vectors
        self.input_dimensions = input_dimensions
        self.LSH_hash = []
        self.adjacency = {}
        self.m = 1  # number of permutations of the hashes
        
        # Load the name variants file
        self.dict_name_variants = json.load(open('../data/name-variants.json'))
        

    def imshow_adjacency_matrix(self, r=()):
        ''' public wrapper for __dict_imshow '''
        self.__dict_imshow(self.adjacency, rng=r)
        
    def __dict_imshow(self, dict1, rng):
        ''' This function draws a plot similar to imshow for a sparse matrix
            represented as a dictionary of x:y key-values.
            Designed to be used with sparse adjacency matrices produced by
            the method get_nearest_neighbors '''
        X, Y = [], []
        
        if rng: x_values = range(rng[0], rng[1]); 
        else: x_values = dict1
        for x in x_values:
            for y in dict1[x]:
                X.append(x)
                Y.append(y)
        pl.scatter(np.array(X), np.array(Y), marker='.')
        ax = pl.gca()
#         ax.set_ylim(ax.get_ylim()[::-1])
        
    def __dict_to_mat(self, dict1):
        maxes = []
        key_max = 0
        for s in dict1:
            if s > key_max: key_max = s
            if dict1[s]: maxes.append(max(dict1[s]))
        M = max(maxes)
        M = max(M, key_max)
        ADJ = np.zeros([M, M])
        for s in dict1:
            for u in dict1[s]:
                ADJ[s - 1, u - 1] = 1
        return ADJ
    
    def __update_dict(self, dict1, dict_add):
        ''' This function updates dict1 in place so that the corresponding values of dict_add are
            added to and merged with the values in dict1'''
    
        for s in dict1:
            dict1[s] = list(set(dict1[s] + dict_add[s]))
            
    def print_list_of_vectors(self):
        # print all vectors
        for vec in  self.list_of_vectors:
            print vec
    
    def __is_close_enough(self, v1, v2):
        ''' This function returns True or False indicating whether two name vectors are close enough to be considered identical or not '''
        ''' If one has no address, then comparison should be much more strict.
            If both have addresses, then addresses should be idenetical, and other fields must be close enough.'''
        
        identical = True
        # Generate dictionary of lastname, firstname, middlename and suffix tokens for both vectors
        dict1 = {}
        dict1[1] = []  # Last name
        dict1[2] = []  # First name
        dict1[3] = []  # Middle name
        dict1[4] = []  # suffix        
       
        for index in v1:
            token = self.index_2_token[index]
            if token[0] not in dict1: dict1[token[0]] = []
            dict1[token[0]].append(token[1])
        
        dict2 = {}
        dict2[1] = []  # Last name
        dict2[2] = []  # First name
        dict2[3] = []  # Middle name
        dict2[4] = []  # suffix        
        for index in v2:
            token = self.index_2_token[index]
            if token[0] not in dict2: dict2[token[0]] = []
            dict2[token[0]].append(token[1])
        
        # If either has no address, require firstname and last name to be identical and also middle name if both have it.
        if not dict1[5] or not  dict2[5]:
            identical = (dict2[1] == dict1[1]) and (dict2[2] == dict1[2]) and ((dict2[3] == dict1[3]) if (dict2[3] and dict1[3]) else True )
            return identical
             
        # IF BOTH HAVE ADDRESSES:
        
        # if both have middlenames, they should be the same
        if dict1[3] and dict2[3]:
            if dict1[3] != dict2[3]: identical = False
        
        # if 1 doesn't have a middle name but 2 does, then 2 is not the "parent" of 1
        if not dict1[3] and dict2[3]: identical = False
          
        # if last names arden't close enough, fail.
#         if dict1[1] != dict2[1]: identical = False;
        if not dict2[1] or not dict1[1]: 
            identical = False       
        elif editdist.distance(dict1[1][0], dict2[1][0]) > 2: identical = False

        # if first names don't overlap, then check if they are variants. If not, fail
#         if not any(i in dict1[2] for i in dict2[2]): identical = False
        firstname1 = ' '.join(dict1[2])
        firstname2 = ' '.join(dict2[2])  
        if editdist.distance(firstname1,firstname2) > 2:
            if firstname2 in self.dict_name_variants:
                if firstname1 not in self.dict_name_variants[firstname2]: 
                    identical = False
            else:
                identical = False
        
        
        
        # If street addresses aren't identical, then fail
        if dict1[5] != dict2[5]: 
            identical = False;
            return identical
        
        return identical
            
        
                
    
    def get_nearest_neighbors(self, B, sigma):
        ''' given a list of strings or hashes, this function computes the nearest
            neighbors of each string among the list.

            Input:
                self.LSH_hash: a list of strings of same length. The strings comprise 0 and 1/
                B: an even integer. The number of adjacent strings to check for proximity for each string
                sigma: float in [0,1], proximity threshold; maximum fraction of all digits in the two strings
                    that can differ for them to be considered neighbors.
            Output: dictionary of indices. For each key (string index), the value is a list of indices
                    of all its nearest neighbors
        '''
        list_of_hashes_sorted = sorted(self.LSH_hash)
        sort_indices = argsort(self.LSH_hash)
#         print sort_indices
        # should be optimized. redundant sorting performed
        
        # number of hash strings in list_of_hashes
        n = len(self.LSH_hash)
        
        # length of each hash string
        m = len(self.LSH_hash[0])
        
        dict_neighbors = {}
             
        for s, i in zip(list_of_hashes_sorted, range(n)):
            dict_neighbors[sort_indices[i]] = []
            # for entry s, find the B nearest entries in the list
            j_low , j_high = i - B / 2, i + B / 2
            if (i - B / 2 < 0):
                j_low, j_high = 0, B
            if (i + B / 2 > n - 1):
                j_low, j_high = n - 1 - B, n - 1
            
            iteration_indices = range(j_low, j_high + 1)
            iteration_indices.remove(i)
            for j in iteration_indices:
                # i,j: current index in the sorted has list
                # sort_indices[i]: the original index of the item residing at index i of the sorted list
#                 if Hamming_distance(list_of_hashes_sorted[i], list_of_hashes_sorted[j]) < sigma * m:
                if self.__is_close_enough(self.list_of_vectors[sort_indices[i]], self.list_of_vectors[sort_indices[j]]):
                    dict_neighbors[sort_indices[i]].append(sort_indices[j])
        return dict_neighbors      
    
    def compute_LSH_hash(self, hash_dim):
        ''' Input:
                list_of_vectors:    list of vectors. Each vector is a dictionary {vector coordinate index, value}
                hash_dim:    dimension of the generated hash.
                dimensions: dimension of the input vectors (this is necessary because the vectors come in a sparse form)
            Output:
                list of hashes of the vectors. Each hash is an m-tuple.
        '''
        dimensions = self.input_dimensions
        
        self.hash_dim = hash_dim
        
        # Number of vectors
        N = len(self.list_of_vectors)
        
        LSH_hash = ['' for i in range(N)]
        
        # generate hash_dim random probe vectors and compute 
        # their inner products with each of the input vectors.
        for k in range(hash_dim):
    #        
            # random "probe" vector 
            vec_tmp = random_uniform_hyperspherical(dimensions)
            
            # convert to sparse form
            vec = sparse_vector(vec_tmp)
            vec_n = vec_norm(vec)
            for v, i in zip(self.list_of_vectors, range(N)):
    #             c = 1 if inner_product(v, vec) / vec_n / vec_norm(v) > 0.0001 else 0
                c = '1' if inner_product(v, vec) > 0 else '0'
                LSH_hash[i] += c
#         return LSH_hash
        self.LSH_hash = LSH_hash
        
        
    def save_LSH_hash(self, filename=None):
        if not filename: filename = '../results/' + self.batch_id + '-LSH_hash.txt'
        f = open(filename, 'w')
        for s in  self.LSH_hash:
            f.write(s + "\n")
        f.close()
        print "Lsh_hash save to file " + filename;
        
    def initialize_adjacency(self):
        ''' This function creates an empty self.adjacency dictionary and then populates it initially as follows:
        it goes over the list of (sorted) hashes and amonge adjacent entriesm it finds maximal groups of identical
        hashes, and creates complete subgraphs corresponding to them in self.adjacency.
        This is necessary if the original list_of_vectors contains repeated entries, such as when we are dealing
        with multiple transactions per person. '''
        self.adjacency = {} 
        i = 0
        while i < len(self.list_of_vectors):
            self.adjacency[i] = []
            j = i + 1
            
            current_group = [i]
            while (j < len(self.list_of_vectors)) and (self.LSH_hash[i] == self.LSH_hash[j]):
                    current_group.append(j)
                    j += 1
            print current_group
            for index in current_group:
                print '-->', index
                self.adjacency[index] = list(current_group)
                self.adjacency[index].remove(index)
                print '    ', index, self.adjacency[index]
            i = j
            
                    # set neighbors of all items in current group and move on to the next item
                    
                    
        
    def compute_similarity(self, B1=30, m=100, sigma1=0.2):                
        # permutate the hash strings m times
        # then create dictionary of B nearest neighbors with distance threshold sigma = 0.7
        # sigma is the fraction of digits in hash that are equal between the two strings
        self.m = m
        print "B=", B1
#         raw_input()
        print "Computing adjacency matrix"
        print "sigma = ", sigma1
#         self.adjacency = self.get_nearest_neighbors(B, sigma)
        n = len(self.LSH_hash)
        
        
#         for i in range(n): self.adjacency[i]=[]
#         pl.ion()

        # Generate an adjacency dictionary and initially populate it with links between records that are identical
        print 'Initialize adjacency...'
        self.initialize_adjacency()
        print 'Done...'
        
        for i in range(m):
            shuffle_list_of_str(self.LSH_hash)
#             print self.LSH_hash[0]
            print 'Shuffling list of hashes and computing nearest neighbors' + str(i)
            adjacency_new = self.get_nearest_neighbors(B=B1, sigma=sigma1)        
            self.__update_dict(self.adjacency, adjacency_new)
#             pl.cla()
#             self.imshow_adjacency_matrix()
#             pl.draw()
#             raw_input("Press Enter to continue...")
             
        print "Adjacency matrix computed!"
            
    
    
    def print_adjacency(self):
        ''' print the similarity matrix (adjacency) '''
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.adjacency)


    def show_sample_adjacency(self):
        ''' Show a random sample of far-off-diagonal elements of the adjaceqncy matrix and
        the content of the corresponding vectors. ''' 
        counter = 0
        if not self.adjacency: return
        N = len(self.list_of_vectors)
        while counter < 20:
            x = random.randint(0, N - 1)
            
            if len(self.adjacency[x]) == 0  :continue
            j = random.randint(0, len(self.adjacency[x]) - 1) if len(self.adjacency[x]) > 1 else 0
            y = self.adjacency[x][j]
            if x == 0: continue
            if 1.0 * (x - y) / x < 0.1:
                counter += 1
                
                print x, y, '-----', self.list_of_vectors[x], self.list_of_vectors[y]
                print self.adjacency[x]
                print self.LSH_hash[x], '\n', self.LSH_hash[y], "\n", Hamming_distance(self.LSH_hash[x], self.LSH_hash[y]) * 1.0 / self.hash_dim, "\n\n"
           
          






        
def covariance(list_of_strs):
    n = len(list_of_strs)
    m = len(list_of_strs[0])
    M = np.zeros([n, n])
    for i in range(n):
        for j in range(i, n):
            M[i, j] = sum([list_of_strs[i][k] == list_of_strs[j][k] for k in range(m)])
    return M


def Hamming_distance(s1, s2):
    '''This function computes the Hamming distance between two strings'''
    return sum([c1 != c2 for c1, c2 in zip(s1, s2)])
    

def random_uniform_hyperspherical(n):
    vec = np.zeros([n, ])
    for i in range(n):
        vec[i] = random.gauss(0, 1)
    vec = vec / np.linalg.norm(vec)
    return vec
        
def shuffle_list_of_str(list_of_strs):
    ''' This function takes a list of strings (of equal lengths) and
        applies the same random permutation to all of them, in place.'''
    n = len(list_of_strs[0])
    l = range(n)
    random.shuffle(l)
    for j in range(len(list_of_strs)):
        list_of_strs[j] = ''.join([list_of_strs[j][l[i]] for i in range(n) ])
    

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



if __name__ == '__main__':    
    # Number of random vectors to generate
    N = 100
    
    # dimension of input vectors
    dim = 40000
    
    # desired dimension (length) of hashes
    hash_dim = 100
    
    # Number of times the hashes are permutated and sorted
    no_of_permutations = 100
    
    # Hamming distance threshold for adjacency 
    sigma = 0.2
    
    # Number of adjacent hashes to compare
    B = 10
    
    list_of_vectors = generate_rand_list_of_vectors(N, dim)
    
    D = Disambiguator(list_of_vectors, dim)
    
    # compute the hashes
    print "Computing the hashes..."
    D.compute_LSH_hash(hash_dim)
    print "Hashes computed..."
    
    print 'Computing similarity matrix...'
    D.compute_similarity(B=10, m=no_of_permutations , sigma=0.2)
    print 'Done...'
    
    
        
    D.show_sample_adjacency()
    
    
    
    pl.subplot(1, 1, 2)
    D.imshow_adjacency_matrix()
    # pl.subplot(1, 2, 1)
    # pl.imshow(ADJ, interpolation='none', cmap='gray')
    pl.show()
        


