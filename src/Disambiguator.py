# import visual as vs 
''' This class receives a list of records and computes a similarity matrix between the nodes. Each record has a "vector".
    Each vector is a dictionary {index:(0 or 1)} where index is a unique integer representing a feature or token in the record.
    The tokens themselves can be specified via index_2_token and token_2_index.

    The core action of the class is by finding candidates for approximate nearest neighbors for each vector based simply on euclidean
    distance.

    This can be further augmented by overriding  the member function __is_close_enough(v1,v2). This functions takes two vectors as inputs
    and decides if they should be linked or not, and uses additional information about the features/tokens as well. This is where
    index_2_token and token_2_index can be used.
    matching_mode can be "strict" or "thorough"
    ''' 

import igraph
import json
from multiprocessing.process import Process
from multiprocessing.queues import Queue
from numpy import math
import os
import pprint
import random
import time

from Affiliations import AffiliationAnalyzer
from Database import DatabaseManager
from Person import Person
from Tokenizer import TokenData, Tokenizer
from Town import Town
import editdist
import numpy as np
import pylab as pl







class Disambiguator():
    def __init__(self, list_of_records, vector_dimension, matching_mode="strict", num_procs=3):

        self.list_of_records = list_of_records
        self.sort_list_of_records()
        
        # dimension of the input vectors (this is necessary because the vectors come in a sparse form)
        self.input_dimensions = vector_dimension
        self.LSH_hash = []
        
        # Now, this is dictionary {index:set of indexes}. It will be augmented in place with each call to self.update_nearest_neighbors()
        # the indexes in the adjacency matrix are the locations of records in the self.list_of_records list.
        # NOT the id of the records! 
        self.index_adjacency = {}
        self.m = 1  # number of permutations of the hashes
        
        # Load the name variants file
        f = open('../data/name-variants.json')
        self.dict_name_variants = json.load(f)
        f.close()
        
        
        self.debug = False
        self.verbose = True
        
        self.project = None
        self.matching_mode = matching_mode
        
        # Number of new matches found in each update of the adjacency matrix.
        # Should be reset to 0 at the beginning of each call to self.update_nearest_neighbors
        self.match_count = 0
        self.adjacency_edgelist = []
       
        # A set() container for the identities (Person objects) 
#         self.set_of_persons = set()
        
        # A set of new matched id pairs (tuples). This is a private field and can be reset after each call to self.update_nearest_neighbors()
        self.new_match_buffer = set()
        
        
        self.tokenizer = None
        
        # A Town object containing a {id(person) : person} dictionary and add/remove methods
        # Persons will be added to the town when identities are extracted from the list of records
        self.town = Town()
        
        # Whether or not to log statistics of record pair comparisons. Use self.set_logstats() to initialize these.
        self.do_stats = False
        
        self.logstats_file = None
        self.logstats_filename = ''
        self.logstats_count = 0
        self.logstats_header = []
        # set of record.id pairs already logged
        self.logstats_set_pairs = set()
        
        if self.do_stats:
            self.set_logstats(True)
            
        self.num_procs = num_procs
        


    
    def sort_list_of_records(self, orderby='id'):
        '''
        sord the records in place by their id (other field(s)). The default ordering is by r.id which makes it easier to reconstruct list_of_records from a partitioning of it.
        Usefull mainly for multi-processing.
        '''
        if orderby == 'id':
            self.list_of_records.sort(key=lambda r: r.id)
        elif orderby == 'name':
            self.list_of_records.sort(key=lambda r: r['NAME'])


            
                
    def set_logstats(self, is_on=True):
        ''' initialize instance attributes needed to log record link statistics'''
        self.do_stats = is_on
        
        if self.do_stats:
            # open output buffer (of size 100kb) for the statistics 
            self.logstats_filename = 'stats-' + str(time.time()) + ".txt"
            self.logstats_file = open(self.logstats_filename, 'w', 100000)
            self.logstats_file.write('#\n' * 5)
            self.logstats_count = 0
            self.logstats_header = []
        
            # set of record.id pairs already logged
            self.logstats_set_pairs = set()
        else:
            try:
                self.logstats_file.close()
            except:
                pass
        
            
        



    @classmethod
    def getCompoundDisambiguator(cls, list_of_Ds, num_procs=1):
        ''' Receive a list of Disambiguator objects and combine them into one fresh object'''
        # Records of each D have a separate TokenData object. Those need to be combined
        # Each record has a vector. Those need to be updated according to the new unified TokenData
        # Each D has a separate G_employer and G_occupation. They need to be combined.
        # update self.input_dimension 
        # TODO: Towns need to be combined too
        
        D_new = Disambiguator([], 0, num_procs=num_procs)
        
        
        # Matching mode. Only set if they are all the same
        set_tmp = {D.matching_mode for D in list_of_Ds}
        if len(set_tmp) > 1:
            raise Exception('To combine D objects, all must have the same matching mode')
        else:
            D_new.matching_mode = set_tmp.pop()
        
        
        # Combine all set_of_persons objects
        for D in list_of_Ds:
#             D_new.set_of_persons = D_new.set_of_persons.union(D.set_of_persons)
            D_new.town = D_new.town.merge(D.town)
        
        # TokenData classmethod that returns a compound TokenData object
        list_of_tokendata = [D.list_of_records[0].tokendata for D in list_of_Ds]
        tokendata = TokenData.getCompoundTokenData(list_of_tokendata)
        
        D_new.tokenizer = Tokenizer()
        D_new.tokenizer.tokens = tokendata 
        D_new.input_dimensions = D_new.tokenizer.tokens.no_of_tokens
        
        # combine affiliation graphs
        list_G_employer = [G_employer for D in list_of_Ds for G_employer in D.list_of_records[0].list_G_employer ]
        list_G_occupation = [G_occupation  for D in list_of_Ds for G_occupation  in D.list_of_records[0].list_G_occupation  ]

        
        for D in list_of_Ds:
            for record in D.list_of_records:
                
                # instance method that translates the vector according to the new tokendata and then replaces the old tokendata
                record.updateTokenData(tokendata)
                
                # Set the records' affiliation graph lists.
                record.list_G_employer = list_G_employer
                record.list_G_occupation = list_G_occupation
                
                # add record to the new D's list_of_records
                D_new.list_of_records.append(record) 
        
        D_new.sort_list_of_records()        
        return D_new
        
                
            
            
        

    def imshow_adjacency_matrix(self, r=()):
        ''' public wrapper for __dict_imshow '''
        self.__dict_imshow(self.index_adjacency, rng=r)
        
    def __dict_imshow(self, dict1, rng):
        ''' This function draws a plot similar to imshow for a sparse matrix
            represented as a dictionary of x:y key-values.
            Designed to be used with sparse index_adjacency matrices produced by
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
    
    def __update_dict_of_sets(self, dict1, dict_add):
        ''' This function updates dict1 in place so that the corresponding values of dict_add are
            added to and merged with the values in dict1.
            It's assumed that the values of the dicts are sets.
            '''
        for s in dict1:
            try:
                dict1[s] = dict1[s].union(dict_add[s])
            except KeyError:
                continue
            
    def print_list_of_vectors(self):
        # print all vectors
        for record in  self.list_of_records:
            print record.vector
 
 
    
    def get_name_pvalue(self, record, which='firstname'):
        '''Return the p-value of the firstname-lastname combination given the
        null hypothesis that first names and last names are selected randomly
        in such a way that the token frequencies are what we observe.'''
        try:
            if which == 'firstname':
                f2 = record.tokendata.get_token_frequency((2, record['N_first_name']))
                total = record.tokendata.no_of_tokens
                return 1.0 * f2 / total 
            
            if which == 'lastname':
                f1 = record.tokendata.get_token_frequency((1, record['N_last_name']))
                total = record.tokendata.no_of_tokens
                return 1.0 * f1 / total 

        except Exception as e:
            print e
            return None
            
        
    
    
    def logstats(self, record1, record2, verdict, result):
        ''' Log statistics for the two compared records.'''
        if not self.do_stats: return
        if self.logstats_count == 0:
            self.logstats_header = sorted(result.keys())
            line = ' '.join(['id1', 'id2', 'verdict', 'p-value-firstname', 'p-value-lastname'] + self.logstats_header) + "\n"
        else:
            
            # Skip if already logged
            if (record1.id, record2.id) in self.logstats_set_pairs or (record2.id, record1.id) in self.logstats_set_pairs: return
            
            pvalue_lastname = None
            pvalue_firstname = None
            
            # If last names identical, compute last name pvalue
            if record1['N_last_name'] == record2['N_last_name']:  
                try:
                    pvalue_lastname = self.get_name_pvalue(record1, which='lastname') 
                    pvalue_lastname = math.log(pvalue_lastname)
                except TypeError:
                    # one of the pvalues is None
                    pvalue_lastname = 'NONE'
                except ValueError:
                    pvalue_lastname = 'NONE'
            
            # If first names identical, compute first name pvalue
            if record1['N_first_name'] == record2['N_first_name']:  
                try:
                    pvalue_firstname = self.get_name_pvalue(record1, which='firstname') 
                    pvalue_firstname = math.log(pvalue_firstname)
                except TypeError:
                    # one of the pvalues is None
                    pvalue_firstname = 'NONE'
                except ValueError:
                    pvalue_firstname = 'NONE'
            
            if pvalue_firstname is None and pvalue_lastname is None: return
                    
            
            line = ' '.join([str(record1.id), str(record2.id), str(int(verdict)), str(pvalue_firstname), str(pvalue_lastname)] + [str(int(result[field])) if result[field] is not None else 'None' for field in self.logstats_header]) + "\n"
            
            self.logstats_set_pairs.add((record1.id, record2.id)) 
        

        self.logstats_file.write(line)
        self.logstats_count += 1
    
    
  
        
        
    def chunk_padded_list_of_records(self, num_chunks, overlap):
        '''
        Divide self.list_or_records (which is assumed to be sorted according to the LSH Hashes)
        into chunks of equal size, and as each one is generated, delete the corresponding slice
        from self.list_of_records.
        output:
            A list of dictionaries, where each dictionary X maps an integer to a record
            such that X[i] = self.list_of_records[i] (with self.list_of_records now sorted according to the hashes)
            as defined in update_nearest_neighbors().

        '''
        n = len(self.list_of_records)

        size = n / num_chunks
        print "n: ", n
        print "size: ", size
        print "overlap: ", overlap
        output = []
        
        block_counter = 0 
        
        while True:
            index_offset = block_counter * (size)
            output.append({i + index_offset : r for i, r in enumerate(self.list_of_records[:size + overlap])})
            if size + overlap >= len(self.list_of_records):
                del self.list_of_records[:size + overlap ]
                break
            else:
                del self.list_of_records[:size ]
                block_counter += 1
        # print output[0].keys()
        # print "_"*80
        # print output[1].keys()
        # quit()
        return output



    def reconstruct_list_of_records(self, list_chunks, overlap):
        '''
        Reconstruct self.list_of_records by concatenating the values of the dictionaries in list_chunks.
        list_chunks is assumed to be generated by self.chunk_padded_list_of_records()
        Note: the chunks in list_chunks overlap. You must take this into account.
        '''
        if self.list_of_records:
            print "ERROR: self.list_of_records is not empty. Can't reconstruct it."
            quit()
    
        print "total entries in list_chunks: ", sum([len(chunk)  for chunk in list_chunks])

        while list_chunks:
            chunk = list_chunks[0]
            num_chunks = len(list_chunks)
            chunk_size = len(chunk)
            # A chunk is a dictionary
            counter = 0

            # index of the last record in the current chunk
            index_max = max(chunk.keys())
                
            for key, record in chunk.iteritems():
                # The last chunk's tail doesn't overlap with another chunk. Include the whole chunk.
                if num_chunks != 1: 
                    if key > index_max - overlap : continue
                self.list_of_records.append(record)
                
            
            # Delete the chunk we just processed from list_chunk.
            del list_chunks[0] 
            
        # Sort the records by their own id.
        self.sort_list_of_records()
        
        tmp = set([r.id for r in self.list_of_records])
        print "NUMBER OF DISTINCT RECORDS IN list_of_records: ", len(tmp) 
            
        
    
    
    def update_nearest_neighbors(self, B, hashes=None, allow_repeats=False, num_procs=None):
        if not num_procs:
            num_procs = self.num_procs
        if (num_procs == 1):
            self.update_nearest_neighbors_single_proc(B, hashes, allow_repeats)
        else:
            self.update_nearest_neighbors_multi_proc(B, hashes, allow_repeats, num_procs)
        
    
    def update_nearest_neighbors_single_proc(self, B, hashes=None, allow_repeats=False):
        ''' given a list of strings or hashes, this function computes the nearest
            neighbors of each string among the list.

            Input:
                hashes: a list of strings of same length. The default is self.LSH_hashes: the strings comprise 0 and 1
                    pass non-default hashes to perform the updating using a custom ordering of the records.
                B: an even integer. The number of adjacent strings to check for proximity for each string

            Output: dictionary of indices. For each key (string index), the value is a list of indices
                    of all its nearest neighbors
        '''
        
        if hashes is None:
            hashes = self.LSH_hash

            
        list_of_hashes_sorted = sorted(hashes)
        sort_indices = argsort(hashes)
        
        # number of hash strings in list_of_hashes
        n = len(hashes)
        
        for s, i in zip(list_of_hashes_sorted, range(n)):
            
            # for entry s, find the B nearest entries in the list
            j_low , j_high = max(0, i - B / 2), min(i + B / 2, n - 1)
#                        
            iteration_indices = range(j_low, j_high + 1)
            iteration_indices.remove(i)
            for j in iteration_indices:
                # i,j: current index in the sorted has list
                # sort_indices[i]: the original index of the item residing at index i of the sorted list
                
                record1, record2 = self.list_of_records[sort_indices[i]], self.list_of_records[sort_indices[j]]
                if  not allow_repeats:
                    if sort_indices[j] in self.index_adjacency[sort_indices[i]]:
                        continue
                
                # If the two records already have identities and they are the same, skip.
                if None != record2.identity == record1.identity != None:
                    continue
                    
                    
                    
                # New implementation: comparison is done via instance function of the Record class
                # Comparison (matching) mode is passed to the Record's compare method.
                verdict, result = record1.compare(record2, mode=self.matching_mode)
                if verdict > 0:
                    self.match_count += 1
                    self.index_adjacency[sort_indices[i]].add(sort_indices[j])
                    self.new_match_buffer.add((sort_indices[i], sort_indices[j]))
                
                # compute some statistics about the records
                if self.do_stats:
                    self.logstats(record1, record2, verdict, result)
                    
 





    def update_nearest_neighbors_multi_proc(self, B, hashes=None, allow_repeats=False, num_procs=None):
        ''' given a list of strings or hashes, this function computes the nearest
            neighbors of each string among the list.

            Input:
                hashes: a list of strings of same length. The default is self.LSH_hashes: the strings comprise 0 and 1
                    pass non-default hashes to perform the updating using a custom ordering of the records.
                B: an even integer. The number of adjacent strings to check for proximity for each string
                num_procs: number of processes to use for this operation.

            Output: dictionary of indices. For each key (string index), the value is a list of indices
                    of all its nearest neighbors
        '''
        '''
            data['pid']: process id
            data['queue']: queue for communication with parent process
            data['matching_mode']: same as self.matching_mode
            data['do_stats']: whether to log stats. same as self.do_stats
            data['index_adjacency']: a sub-dictionary of the full index_adjacency with entries for records relevant to this process.
            data['sort_indices']`
            data['list_indices']: list of indices for the chunk assigned to the child.
            data['B']
            data['allow_repeats']
            data['D_id']: id of the Disambiguator instance that is spawning the child process. This is necessary so that the child worker
                can call processManager and retrieve a reference to the Disambiguator instance that actually spawned it, as a global variable.

            NOTE: the largest piece of data needed by the child processes is self.list_of_records. I no longer pass this to each child
                separately, as it almost doubles the memory used. Instead, I will try to make the disambiguator instance D visible to
                all children as a global variable. They will only read this variable, so the system won't make any copies of it. This
                will hopefully reduce memory usage substantially.

            NOTE: Passing lisrt_of_records to child processes as a global variable isn't going to work. The global namespace will still
                be COPIED to each of the child processes.

        '''
  
 
        if not num_procs: num_procs = self.num_procs


        if hashes is None:
            hashes = self.LSH_hash
        
        # number of hash strings in list_of_hashes
        n = len(hashes)

            



        sort_indices = argsort(hashes)

        sort_indices_dict_inv = {sort_indices[i]:i for i in sort_indices}
        #  Sort self.list_of_records using the hashes as keys. This way, when we're chunking it, we'll have 
        # contiguous pieces, and we can efficiently delete the current chunk from self.list_of_records
        print "Number of records in self.list_of_records: ", len(self.list_of_records)
        permute_inplace(self.list_of_records, sort_indices_dict_inv)
        

        list_procs = []
        list_queues = []

        # Split self.list_of_records into chunks, and truncate it as the split progresses
        # Each element in this list is a DICT X such that X[i] = self.list_of_records[i]
        # Note: after all child prcesses return, list_chunks must be put back together 
        #   to reconstruct self.list_of_records.
        list_chunks = self.chunk_padded_list_of_records(num_procs, B)        
        


        for pid in range(num_procs):
            # print "compiling data for process ", pid
            # list_indices = chunkit_padded(range(n), pid, num_procs, B)
            data = {"pid":pid,
                    "queue": Queue(),
                    "matching_mode": self.matching_mode,
                    "do_stats": self.do_stats,
                    "dict_of_records": list_chunks[pid],
                    "index_adjacency":{sort_indices[j]: self.index_adjacency[sort_indices[j]] for j, dummy in list_chunks[pid].iteritems()},
                    "sort_indices":sort_indices,
                    "B":B,
                    "allow_repeats":allow_repeats}
            p = Process(target=find_nearest_neighbors, args=[data])
            list_procs.append(p)
            list_queues.append(data['queue'])
            p.start()
            # print "started process ", pid
        
        # Receive outputs from processes
        for i, q in enumerate(list_queues):
            result = q.get()
            # print "Received results from process" , i
            # Process the results
            
            # merge result['index_adjacency'] into self.index_adjacency
            self.__update_dict_of_sets(self.index_adjacency, result['index_adjacency'])
                
            # merge result['new_match_buffer'] into self.new_match_buffer
            self.__update_dict_of_sets(self.new_match_buffer, result['new_match_buffer'])
            
            self.match_count += result['match_count']


        
        # join processes
        for p in list_procs:
            # print "joining process", p
            p.join()
        
        print "all processes returned"

        # Reconstruct self.list_of_records from the chunks extracted earlier.
        print "Reconstructing self.list_of_records..."
        self.reconstruct_list_of_records(list_chunks, overlap=B)
        print "\n\n\n\n\n\n\n\n\n\n\n"
      
      
      
      
      
      
    def compute_LSH_hash_single_process(self, hash_dim):
        ''' Input:
                list_of_vectors:    list of vectors. Each vector is a dictionary {vector coordinate index, value}
                hash_dim:    dimension of the generated hash.

            Output:
                list of hashes of the vectors. Each hash is an m-tuple.
        '''
        dimensions = self.input_dimensions
            
        self.hash_dim = hash_dim
        
        # Number of vectors
        N = len(self.list_of_records)
        
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
            for record, i in zip(self.list_of_records, range(N)):
                v = record.vector
                c = '1' if inner_product(v, vec) > 0 else '0'
    #             c = 1 if inner_product(v, vec) / vec_n / vec_norm(v) > 0.0001 else 0
                LSH_hash[i] += c
#         return LSH_hash
        self.LSH_hash = LSH_hash
    
    
    
    
    def compute_LSH_hash(self, hash_dim):
        ''' Input:
                list_of_vectors:    list of vectors. Each vector is a dictionary {vector coordinate index, value}
                hash_dim:    dimension of the generated hash.

            Output:
                list of hashes of the vectors. Each hash is an m-tuple.
        '''
        dimensions = self.input_dimensions
        
        self.hash_dim = hash_dim
        
        # Number of vectors
        N = len(self.list_of_records)
        
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
            for record, i in zip(self.list_of_records, range(N)):
                v = record.vector
                c = '1' if inner_product(v, vec) > 0 else '0'
    #             c = 1 if inner_product(v, vec) / vec_n / vec_norm(v) > 0.0001 else 0
                LSH_hash[i] += c
#         return LSH_hash
        self.LSH_hash = LSH_hash   
        
        
    def save_LSH_hash(self, filename=None, batch_id=0):
        if not filename: filename = '../results/' + batch_id + '-LSH_hash.txt'
        f = open(filename, 'w')
        for s in  self.LSH_hash:
            f.write(s + "\n")
        f.close()
        print "Lsh_hash save to file " + filename;
        
    def initialize_index_adjacency(self):
        ''' This function creates an empty self.index_adjacency dictionary and then populates it initially as follows:
        it goes over the list of (sorted) hashes and among adjacent entries it finds maximal groups of identical
        hashes, and creates complete subgraphs corresponding to them in self.index_adjacency.
        This is necessary if the original list_of_vectors contains repeated entries, such as when we are dealing
        with multiple transactions per person. '''
        self.index_adjacency = {} 
        i = 0
        while i < len(self.list_of_records):
            self.index_adjacency[i] = set()
            j = i + 1
            
            current_group = [i]
            while (j < len(self.list_of_records)) and (self.LSH_hash[i] == self.LSH_hash[j]):
                    current_group.append(j)
                    j += 1
            if self.debug: print current_group
            for index in current_group:
                self.index_adjacency[index] = set(current_group) 
                self.index_adjacency[index].remove(index)
                if self.debug:
                    print '-->', index
                    print '    ', index, self.index_adjacency[index]
            i = j
        
        # update nearest neighbors once with the initial ordering of the records (before hashes are calculated and sorted)
        # self.update_nearest_neighbors(B=100, hashes=xrange(len(self.list_of_records)))

        # Perform the update with list_of_records sorted by names. To do this, all we have to do 
        # Is use the names themselves as the hashes!
        self.update_nearest_neighbors(B=100, hashes=[x['NAME'] for x in self.list_of_records])


            
    # Convert self.index_adjacency to an edgelist
    def compute_edgelist(self):

        # place self-links 
        self.index_adjacency_edgelist = [(i, i) for i in self.index_adjacency] 
        
        for index in self.index_adjacency:
            for neighbor in self.index_adjacency[index]:
                self.index_adjacency_edgelist.append((index, neighbor))
    
    
    def compute_similarity(self, B1=30, m=100, sigma1=0.2):                
        # permutate the hash strings m times
        # then create dictionary of B nearest neighbors with distance threshold sigma = 0.7
        # sigma is the fraction of digits in hash that are equal between the two strings
        self.m = m
        print "B=", B1
        print "Computing index_adjacency matrix"
        print "sigma = ", sigma1
#         self.index_adjacency = self.get_nearest_neighbors(B, sigma)
        n = len(self.LSH_hash)
        
        

        # Generate an index_adjacency dictionary and initially populate it with links between records that are identical
        print 'Initialize adjacency...'
        self.initialize_index_adjacency()
        print 'Done...'
        
        for i in range(m):
            
            self.match_count = 0
            self.update_nearest_neighbors(B=B1)
            print "New matches found: " , self.match_count

            shuffle_list_of_str(self.LSH_hash)
            print 'Shuffling list of hashes and computing nearest neighbors' + str(i)

#             adjacency_new = self.get_nearest_neighbors(B=B1, sigma=sigma1)        
#             self.__update_dict(self.adjacency, adjacency_new)
            
            if self.project:
                self.project.log('Suffling hash list...', str(i))
#         self.compute_edgelist()
        print "index_adjacency matrix computed!"
            
    
    
    def print_index_adjacency(self):
        ''' print the similarity matrix (index_adjacency) '''
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.index_adjacency)


    def show_sample_index_adjacency(self):
        ''' Show a random sample of far-off-diagonal elements of the adjaceqncy matrix and
        the content of the corresponding vectors. ''' 
        counter = 0
        if not self.index_adjacency: return
        N = len(self.list_of_records)
        while counter < 20:
            x = random.randint(0, N - 1)
            
            if len(self.index_adjacency[x]) == 0  :continue
            j = random.randint(0, len(self.index_adjacency[x]) - 1) if len(self.index_adjacency[x]) > 1 else 0
            y = list(self.index_adjacency[x])[j]
            if x == 0: continue
            if 1.0 * (x - y) / x < 0.1:
                counter += 1
                
                print x, y, '-----', self.list_of_records[x].vector, self.list_of_records[y].vector
                print self.index_adjacency[x]
                print self.LSH_hash[x], '\n', self.LSH_hash[y], "\n", Hamming_distance(self.LSH_hash[x], self.LSH_hash[y]) * 1.0 / self.hash_dim, "\n\n"
           
          



    ''' Generate Person objects from the records and the index_adjacency matrix
        The main product is self.town
        TODO: Generate each person's set of possible neighbors'''
    def generate_identities(self):
        self.compute_edgelist()
    
        G = igraph.Graph.TupleList(edges=self.index_adjacency_edgelist)
        clustering = G.components()

        # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
        persons_subgraphs = clustering.subgraphs()
        persons_subgraphs.sort(key=lambda g: min([int(v['name']) for v in g.vs]))

#         self.set_of_persons = set()
        self.town = Town()
        
        # Temp list to preserve the ordering of the persons, so neihbors can be assigned
        list_of_persons = []
        
        # Sort the subgraphs by their smallest node "name"
        for g in persons_subgraphs:
            person = Person()
            
            
            for v in sorted(g.vs, key=lambda v:int(v['name'])):
                index = int(v['name'])
                r = self.list_of_records [index]
                person.addRecord(r)
                

            # TODO: maybe consolidate these?
            self.town.addPerson(person)
#             self.set_of_persons.add(person)
            list_of_persons.append(person)
        
        n = len(list_of_persons)
        
        
        # Assign neighbors. Consider adjacent persons in list_of_persons.
        # TODO: decide if more need to be considered
        # Note: you can't define neighborhood based on compatibility because 
        #     by construction, different persons start out having zero compatibility. 
        for i, person in enumerate(list_of_persons):
            for j in range(i - 3, i + 4):
                if i != j: 
                    try:
                        person.neighbors.add(id(list_of_persons[j]))
                    except IndexError:
                        pass
                        
                    




    '''
    Split identities if the middle names within aren't consistent
    '''

    # print two persons and their compatibility to stdout
    def print_compatibility(self, p1, p2):
        print p1.toString(),
        print "-"*40
        print p2.toString()
        print p1.compatibility(p1, p2)
        print '_' * 80



    
    def print_list_of_persons(self, list_persons, message):
        print "_"*30 + message + "_"*30
        if not list_persons:
            print "NONE"
        for person in list_persons:
            print person.toString()
            print (" "*20 + "|" + "\n") * 3
        print ("="*70 + "\n") * 3
    
    
    # Return a list of persons with multiple states in their timeline
    def get_set_of_persons_multistate(self):
        list_persons = []
        for id, person in self.town.dict_persons.iteritems():
            set_states = person.get_distinct_attribute('STATE')
            if len(set_states) > 1:
                list_persons.append(person)
            
        return list_persons
            
    
    
    def refine_identities_on_MIDDLENAME(self):
        set_new_persons = set()
        set_dead_persons = set()

        for id, person in self.town.dict_persons.iteritems():
            middlenames = person.get_middle_names()
            
            # If there are more than 1 middle names, person needs to split
            if len(middlenames) > 1:

                # set of Persons to replace this one
                spawns = person.split_on_MIDDLENAME()
        
                if self.verbose:        
                    self.print_list_of_persons(spawns, message="Spawns of the person")
                    self.print_list_of_persons(self.town.getPersonsById(person.neighbors), message="Neighbors of the person")
                
                set_stillborn_spawns = set()
                
                # TODO: check that the neighbor isn't already dead
                # Go through all neighbors of the person and decide if the child should be merged with them
                # See if you can merge the spawns with any of the parent's neighbors
                for child in spawns:
#                     print "-------spawn:"
#                     print child.toString()
                    for neighbor in self.town.getPersonsById(person.neighbors):
                        if neighbor.isDead: continue
                        if neighbor.isCompatible(child):
                           
                            self.print_compatibility(neighbor, child)
                            # TODO: update self.index_adjacency as well
                            
                            neighbor.merge(child)
#                             print '----------merging'
#                             print neighbor.toString()
#                             print "-"*20
#                             print child.toString()
#                             print "="*40
                            
                            set_stillborn_spawns.add(child)
                            break
                
#                 print "--------new persons:"
#                 print set(spawns).difference(set_stillborn_spawns),"\n\n"
                
                
                for child in spawns.difference(set_stillborn_spawns):
                    set_new_persons.add(child)
#                     if self.verbose:
#                         print child.toString()
                        
#                 if self.verbose:
#                     print ("="*70 + "\n")*3

                # Schedule exploded person for burial
                set_dead_persons.add(person)
                
                
                
                        
        for person in set_dead_persons:
            # Destroy the exploded person 
            if self.verbose:
                print "DEAD" + "="*70
                print person.toString()  
#             self.set_of_persons.remove(person)
            self.town.removePerson(person)
            person.destroy()


    
        for person in set_new_persons:
            if self.verbose:
                print "BORN" + "="*70
                print person.toString()
#             self.set_of_persons.add(person)
            self.town.addPerson(person)
            
            pass
        
        
        
    
    
    
    '''
    Use self.new_matches_buffer to look for Person objects that are potentially connected.
    If so, merge the Persons.
    ''' 
    def merge_identities(self):
        
        for count, pair in enumerate(self.new_match_buffer):
            print "comparing pair no. %d" % count
            i, j = pair
            r1 = self.list_of_records[i]
            r2 = self.list_of_records[j]
            
            
            
            p1 , p2 = r1.identity, r2.identity
            print "identities:", len(p1.set_of_records), len(p2.set_of_records)
            
            
                        
            # If both records belong to the same person continue
            if p1 is p2:
                continue
            
            # Otherwise compare persons and if compatible, merge
            if p1.isCompatible(p2):
                if self.verbose:
                    print "MERGING two persons" + "="*70
                    print p1.toString()       
                    print "-"*50       
                    print p2.toString()
                    print "="*70
                p1.merge(p2)
#                 self.set_of_persons.remove(p2)
                self.town.removePerson(p2)
            else:
                print "pair rejected"
        pass
            
            
            
        
    def refine_identities_merge_similars(self, m):
        '''
        Run self.update_nearest_neighbors() a few more times; this time consider matches across different Persons
        and if necessary merge the corresponding Persons.
        Parameters:
            m: how many times to shuffle the hashes and recompute
        '''
        if self.project:
            self.project.log('v', 'Looking for similar Persons to merge...')
        for i in range(m):
            self.match_count = 0
            
            # reset the new_math_buffer
            self.new_match_buffer = set()

            print 'Shuffling list of hashes and computing nearest neighbors' + str(i)
            shuffle_list_of_str(self.LSH_hash)
            
            # update index_adjacency AND get a set of new matches that belong to different persons
            self.update_nearest_neighbors(B=20, allow_repeats=True)
            print "New matches found: " , self.match_count
            
            # process the newly found matches and if necessary merge their corresponding Persons
            self.merge_identities() 

            if self.project:
                self.project.log('Suffling hash list...', str(i))
#         self.compute_edgelist()
        if self.project:
            self.project.log("index_adjacency matrix computed!", 'm')
            
        
   
        
    
    '''
    Refine the list of Persons: split, merge, etc.
    '''
    def refine_identities(self):
        
        self.refine_identities_on_MIDDLENAME()
        
        print "Merging similar persons..." 
        self.refine_identities_merge_similars(5)
        
                
        
    def generator_identity_list(self):
        ''' generate tuples: [(record_id, Person_id)].
        person id is an integer that's unique among the persons in this D.'''
        list_persons = self.town.getAllPersons()
        list_persons.sort(key=lambda person: min([record['N_last_name'] for record in person.set_of_records]))
        for i, person in enumerate(list_persons):
            for record in person.set_of_records:
                yield (record.id, i)
                
    
    def save_identities_to_db(self):
        '''Export the calculated identities of the records to a database table called "identities".'''
        db_manager = DatabaseManager()    

        db_manager.runQuery('DROP TABLE IF EXISTS identities;')
        db_manager.runQuery('CREATE TABLE identities ( id INT PRIMARY KEY, identity INT);')
        
        for id_pair in self.generator_identity_list():
            print id_pair
            result = db_manager.runQuery('INSERT INTO identities (id,identity)  VALUES (%d,%d);' % id_pair)
            print result
        db_manager.connection.commit()
        db_manager.connection.close()
                






def permute_inplace(X, Y):
    ''''
    permute the list X inplace, according to Y.
    Y is a dictionary {c_index : t_index } which means the value of X[c_index]
    should end up in X[t_index].
    '''
    while Y:
        # key values to be deleted from Y at the end of each runthrough
        death_row_keys = []
        # Iterate through current indexes
        for c_index in Y:
            # Target index
            t_index = Y[c_index]
            if c_index == t_index:
                death_row_keys.append(c_index)
                continue
            # Swap values of the current and target indexes in X
            # print t_index,c_index
            X[t_index], X[c_index] = X[c_index], X[t_index]
            Y[t_index], Y[c_index] = Y[c_index], Y[t_index]

        for key in death_row_keys:
            del Y[key]










def find_nearest_neighbors(data):
    '''
    The worker function run in each process spawed by Disambiguator.update_nearest_neighbors().
    This function finds the nearest neighbors for all records in a list of records it receives.
    Parameters:
        data: a dict of all the data needed by this worker function.
        data['pid']: process id
        data['queue']: queue for communication with parent process
        data['matching_mode']: same as self.matching_mode
        data['do_stats']: whether to log stats. same as self.do_stats
        data['index_adjacency']: a sub-dictionary of the full index_adjacency with entries for records relevant to this process.
        data['dict_of_records']: a dictionary that maps an index to a record. The indices form a contiguous set of integers, and
            reflect the current ordering of self.list_of_records according the list of hashes. Therefore, here, we can simply
            compare index i with index i+1 throughout the dict_of_records. That is, adjacency of the indices in dict_of_records
            implies adjacency of the corresponding records in list of hashes.
        data['sort_indices']
        data['B']
        data['allow_repeats']
        data['list_indices']: list of the indices (in the sorted hashes list) to be processed by this child

    Returns:
        result: a dict of all output variables.
        result['match_count']: number of matches found by this process
        result['new_match_buffer']: Set of matched record id pairs
        result['index_adjacency']: updated index_adjacency. Will be merged into self.index_adjacency in the calling process.
    '''
    
    # "dict_of_records":{i:self.list_of_records[sort_indices[i]] for i in list_indices},
    
    allow_repeats = data['allow_repeats']
    sort_indices = data['sort_indices']
    # list_indices = data['list_indices']
    B = data['B']
    pid = data['pid']
    matching_mode = data['matching_mode']
    do_stats = data['do_stats']
    index_adjacency = data['index_adjacency']
    dict_of_records = data['dict_of_records']

    n = len(dict_of_records)
    
    output = {'match_count':0,
              'new_match_buffer':set(),
              'index_adjacency':index_adjacency}

    # i_min, i_max = min(list_indices), max(list_indices)
    
    list_indices = dict_of_records.keys()
    list_indices.sort()
    i_min, i_max = min(list_indices), max(list_indices)


    # print pid,"--------", sorted(index_adjacency.keys())
    # print pid,"--------", sorted([sort_indices[i] for i in list_indices])
    
    # data['queue'].put(output)
    # return

   
    
    for i in list_indices:
        
        # for entry s, find the B nearest entries in the list
        j_low , j_high = max(i_min, i - B / 2), min(i + B / 2, i_max)
                            
        iteration_indices = range(j_low, j_high + 1)
        iteration_indices.remove(i)
        for j in iteration_indices:
            # i,j: current index in the sorted has list
            # sort_indices[i]: the original index of the item residing at index i of the sorted list
            
            # record1, record2 = dict_of_records[i], dict_of_records[j]
            record1, record2 = dict_of_records[i], dict_of_records[j]
            if  not allow_repeats:
                if sort_indices[j] in output['index_adjacency'][sort_indices[i]]:
                    continue
            
            # If the two records already have identities and they are the same, skip.
            if None != record2.identity == record1.identity != None:
                continue
                
            # New implementation: comparison is done via instance function of the Record class
            # Comparison (matching) mode is passed to the Record's compare method.
            verdict, result = record1.compare(record2, mode=matching_mode)
            if verdict > 0:
                output['match_count'] += 1
                output['index_adjacency'][sort_indices[i]].add(sort_indices[j])
                output['new_match_buffer'].add((sort_indices[i], sort_indices[j]))
            
            # compute some statistics about the records
            # Forget about logstats for now
            # if do_stats:
            #    logstats(record1, record2, verdict, result)
    data['queue'].put(output)
    
      



def chunkit_padded(list_input, i, num_chunks, overlap=0):
    '''
    This function splits a list into "total_count" sublists of roughly equal sizes and returns the "ith" one.
    These lists are overlapping.
    '''
    n = len(list_input)

    size = n / num_chunks

    x, y = i * size, min((i + 1) * size + overlap, n)
    return list_input[x:y]




        
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
    

def argsort_list_of_dicts(seq, orderby):
    ''' argsort for a sequence of dicts or dict subclasses. Allows sorting by the value of a given key of the dicts.
         returns the indices of the sorted sequence'''
    if not orderby:
        raise Exception("Must specify key to order dicts by.") 
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=lambda index: seq.__getitem__(index)[orderby])





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


