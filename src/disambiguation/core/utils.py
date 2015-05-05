'''
This module loads commonly used packages and modules.
'''


import json
import pprint
import cPickle
import datetime
import glob
import igraph
import multiprocessing 
import os
import pickle
import random
import re
import sys
import time
import numpy as np
import pandas as pd
import states
from .. import config
import math


def partition_integer(N, n):
    '''
    Partition an integer number into n roughly equal integers.
    Return these integers in a list. N = n_1 + n_2 + ... + n_n.
    @param N: the integer to be partitioned
    @param n: the number of partitions.
    '''
    a = np.round((float(N) / n) * np.arange(n + 1))
    b = (a[1:] - a[:-1]).astype(int)
    return b



def find_all_in_list(regex, str_list):
    ''' Given a list of strings, str_list and a regular expression, regex, return a dictionary with the
        frequencies of all mathces in the list.'''
    dict_matches = {}
    for s in str_list:
#         s_list = re.findall(r'\b\w\b', s)
        s_list = re.findall(regex, s)
        for s1 in s_list:
            if s1 in dict_matches:
                dict_matches[s1] += 1 
            else:
                dict_matches[s1] = 1
    return dict_matches

 




def get_next_batch_id():
    with  open(config.src_path + '../../config/batches.list') as f:
        s = f.read()
    i = int(s)
    with open(config.src_path + '../../config/batches.list', 'w') as f:
        f.write(str(i + 1))
    return(str(i))





def load_normalized_attributes(state):
    '''
    Load and return normalized attributes for state.
    '''
    filename = config.normalized_attributes_file_template % state
    with open(filename) as f:
        dict_normalized_attributes = cPickle.load(f)
    return dict_normalized_attributes
    
def load_feature_vectors(state, tokenizer_class_name='Tokenizer'):
    '''
    Load and return feature vectors for state and tokenizer class.
    '''
    filename = config.vectors_file_template % (state, tokenizer_class_name)
    with open(filename) as f:
        dict_vectors = cPickle.load(f)
    return dict_vectors


def load_tokendata(state, tokenizer_class_name='Tokenizer'):
    '''
    Load and return tokendata for state and tokenizer class.
    '''
    filename = config.tokendata_file_template % (state, tokenizer_class_name)
    with open(filename) as f:
        tokendata = cPickle.load(f)
    return tokendata
    
    
def load_hashes(state, tokenizer_class_name='Tokenizer'):
    '''
    Load and return hashes for state and tokenizer class.
    '''
    filename = config.hashes_file_template % (state, tokenizer_class_name)
    with open(filename) as f:
        dict_hashes = cPickle.load(f)
    return dict_hashes



def jaccard_similarity(set1, set2):
    '''
    Return the Jaccard similarity of two sets
    '''

    return 1. * len(set1 - set2) / len(set1.union(set2))


def chunks_replace(l, n):
    '''
    split a list into precisely n contiguous chunks of roughly equal size.
    As a chunk is extracted, delete that chunk from l. This is useful when
    working with very large lists where due to memory concerns, we want to
    avoid keeping duplicates of the chunks in memory.

    @param l: list to be split.

    @return: list of contiguous chunks extracted from l.
    '''
    N = len(l)
    size = float(N) / n
    n_removed = 0
    list_chunks = []
    for i in range(n):
        chunk = l[int(i * size) - n_removed : int((i + 1) * size) - n_removed]
        list_chunks.append(chunk)
        del l[int(i * size) - n_removed : int((i + 1) * size) - n_removed]
        n_removed += len(chunk)
    
    return list_chunks



def chunks(l, n):
    '''
    split a list into precisely n contiguous chunks of roughly equal size.

    @param l: list to be split.

    @return: list of contiguous chunks extracted from l.
    '''
    N = len(l)
    size = float(N) / n
    return [l[int(i * size):int((i + 1) * size)] for i in range(n)]

        

def chunkit_padded(list_input, i, num_chunks, overlap=0):
    '''
    This function splits a list into "total_count" sublists of roughly equal sizes and returns the "ith" one.
    These lists are overlapping.
    '''
    n = len(list_input)

    size = float(n) / num_chunks

    x, y = int(math.ceil(i * size)), min(int(math.ceil((i + 1) * size)) + overlap, n)
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
    '''
    Return a hyperspherically random vector in n dimensions.
    '''
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



def chunk_dict(dictionary, num_chunks):
    '''
    Split the dictionary into num_chunks roughly equal sub-dictionaries
    and return a list containing these sub-dictionaries.
    '''
    list_dicts = [{} for i in range(num_chunks)]
    counter = 0
    for key, value in dictionary.iteritems():
        list_dicts[counter % num_chunks][key] = value
        counter += 1
    return list_dicts
        
