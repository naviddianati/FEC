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
from numpy import math
import os
import pprint
import random
import time

from Affiliations import AffilliationAnalyzer
from Database import DatabaseManager
from Person import Person
from Tokenizer import TokenData, Tokenizer
from Town import Town
import editdist
import numpy as np
import pylab as pl


class Disambiguator():
    def __init__(self, list_of_records, vector_dimension, matching_mode="strict"):
        
        
        
        # I Believe these are unnecessary. 
#         self.index_2_token = index_2_token
#         self.token_2_index = token_2_index
#         self.list_of_vectors = list_of_vectors
        self.list_of_records = list_of_records
        
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
        self.verbose = False
        
        self.project = None
        self.matching_mode = matching_mode
        
        # Number of new matches found in each update of the adjacency matrix.
        # Should be reset to 0 at the beginning of each call to self.update_nearest_neighbors
        self.match_count = 0
        self.adjacency_edgelist = []
       
        # A set() container for the identities (Person objects) 
        self.set_of_persons = set()
        
        # A set of new matched id pairs (tuples). This is a private field and can be reset after each call to self.update_nearest_neighbors()
        self.new_match_buffer = set()
        
        
        self.tokenizer = None
        
        # A Town object containing a {id(person) : person} dictionary and add/remove methods
        # Persons will be added to the town when identities are extracted from the list of records
        self.town = Town()
        
        # Whether or not to log statistics of record pair comparisons
        self.do_stats = False
        
        # open output buffer (of size 100kb) for the statistics 
        if self.do_stats:
            self.logstats_file = open('stats.txt', 'w', 100000)
        self.logstats_count = 0
        self.logstats_header = []

            
            
            




    @classmethod
    def getCompoundDisambiguator(cls, list_of_Ds):
        ''' Receive a list of Disambiguator objects and combine them into one fresh object'''
        # Records of each D have a separate TokenData object. Those need to be combined
        # Each record has a vector. Those need to be updated according to the new unified TokenData
        # Each D has a separate G_employer and G_occupation. They need to be combined.
        # update self.input_dimension 
        # TODO: Towns need to be combined too
        
        D_new = Disambiguator([], 0)
        
        
        # Matching mode. Only set if they are all the same
        set_tmp = {D.matching_mode for D in list_of_Ds}
        if len(set_tmp) > 1:
            raise Exception('To combine D objects, all must have the same matching mode')
        else:
            D_new.matching_mode = set_tmp.pop()
        
        
        # Combine all set_of_persons objects
        for D in list_of_Ds:
            D_new.set_of_persons = D_new.set_of_persons.union(D.set_of_persons)
        
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
    
    def __update_dict(self, dict1, dict_add):
        ''' This function updates dict1 in place so that the corresponding values of dict_add are
            added to and merged with the values in dict1
            NOT BEING USED ANYMORE. Now, self.index_adjacency is updated in-place.
            '''
        for s in dict1:
            dict1[s] = list(set(dict1[s] + dict_add[s]))
            
    def print_list_of_vectors(self):
        # print all vectors
        for record in  self.list_of_records:
            print record.vector
 
 
    
    def get_name_pvalue(self, record):
        '''Return the p-value of the firstname-lastname combination given the
        null hypothesis that first names and last names are selected randomly
        in such a way that the token frequencies are what we observe.'''
        try:
            f2 = record.tokendata.get_token_frequency((2, record['N_first_name']))
            f1 = record.tokendata.get_token_frequency((1, record['N_last_name']))
            total = record.tokendata.no_of_tokens
            return 1.0 * f1 * f2 / total / total
        except Exception as e:
            print e
            return None
            
        
    
    
    def logstats(self, record1, record2, verdict, result):
        ''' Log statistics for the two compared records.'''
        if self.logstats_count == 0:
            self.logstats_header = sorted(result.keys())
            line = ' '.join(['id1', 'id2', 'verdict', 'p-value'] + self.logstats_header) + "\n"
        else:
            try:
                pvalue = self.get_name_pvalue(record1) * self.get_name_pvalue(record2)
                pvalue = math.log(pvalue)
            except TypeError:
                # one of the pvalues is None
                pvalue = 'NONE'
            except ValueError:
                pvalue = 'NONE'
            line = ' '.join([str(record1.id), str(record2.id), str(int(verdict)), str(pvalue)] + [str(int(result[field])) if result[field] is not None else 'NONE' for field in self.logstats_header]) + "\n"

        

        self.logstats_file.write(line)
        self.logstats_count += 1
    
    
    def update_nearest_neighbors(self, B, hashes=None, allow_repeats=False):
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
#         print sort_indices
        # should be optimized. redundant sorting performed
        
        # number of hash strings in list_of_hashes
        n = len(hashes)
        
        # NOT USED length of each hash string
        # m = len(hashes[0])
        
        # Now I'm directly updating self.index_adjacency. This won't be necessary
        # dict_neighbors = {}
             
        for s, i in zip(list_of_hashes_sorted, range(n)):
            # Now I'm directly updating self.index_adjacency. This won't be necessary
            # dict_neighbors[sort_indices[i]] = []
            
            # for entry s, find the B nearest entries in the list
            j_low , j_high = max(0, i - B / 2), min(i + B / 2, n - 1)
#             if (i - B / 2 < 0):
#                 j_low, j_high = 0, B
#             if (i + B / 2 > n - 1):
#                 j_low, j_high = n - 1 - B, n - 1
            
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
                    
#                 if (sort_indices[i], sort_indices[j]) not in self.dict_already_compared_pairs:
#                     if self.list_of_records[sort_indices[i]].compare(self.list_of_records[sort_indices[j]], mode=self.matching_mode):
#                         dict_neighbors[sort_indices[i]].append(sort_indices[j])
#                     self.dict_already_compared_pairs[(sort_indices[i], sort_indices[j])] = 1
#         return dict_neighbors      
    
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
        self.update_nearest_neighbors(B=80, hashes=xrange(len(self.list_of_records)))

            
                    # set neighbors of all items in current group and move on to the next item
                    
                    
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
        The main product is self.set_of_persons
        TODO: Generate each person's set of possible neighbors'''
    def generate_identities(self):
        self.compute_edgelist()
    
        G = igraph.Graph.TupleList(edges=self.index_adjacency_edgelist)
        clustering = G.components()

        # List of subgraphs. Each subgraph is assumed to contain nodes (records) belonging to a separate individual
        persons_subgraphs = clustering.subgraphs()
        
        self.set_of_persons = set()
        
        # Temp list to preserve the ordering of the persons, so neihbors can be assigned
        list_of_persons = []
        
        # Sort the subgraphs by their smallest node "name"
        for g in sorted(persons_subgraphs, key=lambda g: min([int(v['name']) for v in g.vs])):
            person = Person()
            
            
            for v in sorted(g.vs, key=lambda v:int(v['name'])):
                index = int(v['name'])
                r = self.list_of_records [index]
                person.addRecord(r)
                

            # TODO: maybe consolidate these?
            self.town.addPerson(person)
            self.set_of_persons.add(person)
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



    
    def print_set_of_persons(self, list_persons, message):
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
        for person in self.set_of_persons:
            set_states = person.get_distinct_attribute('STATE')
            if len(set_states) > 1:
                list_persons.append(person)
            
        return list_persons
            
    
    
    def refine_identities_on_MIDDLENAME(self):
        set_new_persons = set()
        set_dead_persons = set()
        for person in self.set_of_persons:
            middlenames = person.get_middle_names()
            
            # If there are more than 1 middle names, person needs to split
            if len(middlenames) > 1:

                # set of Persons to replace this one
                spawns = person.split_on_MIDDLENAME()
        
                if self.verbose:        
                    self.print_set_of_persons(spawns, message="Spawns of the person")
                    self.print_set_of_persons(self.town.getPersonsById(person.neighbors), message="Neighbors of the person")
                
                
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
                
                
                
#                 person.destroy()
                
                
                        
        for person in set_dead_persons:
            # Destroy the exploded person 
            if self.verbose:
                print "DEAD" + "="*70
                print person.toString()  
            self.set_of_persons.remove(person)
            self.town.removePerson(person)
            person.destroy()


    
        for person in set_new_persons:
            if self.verbose:
                print "BORN" + "="*70
                print person.toString()
            self.set_of_persons.add(person)
            self.town.addPerson(person)
            
            pass
        
        
        
    
    
    
    '''
    Use self.new_matches_buffer to look for Person objects that are potentially connected.
    If so, merge the Persons.
    ''' 
    def merge_identities(self):
        
        for pair in self.new_match_buffer:
            i, j = pair
            r1 = self.list_of_records[i]
            r2 = self.list_of_records[j]
            
            
            
            p1 , p2 = r1.identity, r2.identity
            
                        
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
                self.set_of_persons.remove(p2)
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
            self.project.log("index_adjacency matrix computed!",'m')
            
        
    # TODO:
    def refine_identities_on_(self):
        set_new_persons = set()
        set_dead_persons = set()
        for person in self.set_of_persons:
            middlenames = person.get_middle_names()
            
            # If there are more than 1 middle names, person needs to split
            if len(middlenames) > 1:
                

                # set of Persons to replace this one
                spawns = person.split_on_MIDDLENAME()
                if self.verbose:
                    self.print_set_of_persons(spawns, message="Spawns of the person")
                    self.print_set_of_persons(self.town.getPersonsById(person.neighbors), message="Neighbors of the person")
                
                
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
                
                
                
#                 person.destroy()
                
                
                        
        for person in set_dead_persons:
            # Destroy the exploded person 
            if self.verbose:
                print "DEAD" + "="*70
                print person.toString()  
            self.set_of_persons.remove(person)
            person.destroy()


    
        for person in set_new_persons:
            if self.verbose:
                print "BORN" + "="*70
                print person.toString()
            self.set_of_persons.add(person)
            
            pass
        
        
    
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
        for i, person in enumerate(self.set_of_persons):
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


