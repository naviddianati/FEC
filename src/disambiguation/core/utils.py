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
from .. import config
import states
import math
from ast import literal_eval
import filters
import resource


# list of all alphanumeric characters
abcd = 'abcdefghijklmnopqrstuvwxz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'

random.seed()



# list of just pieces often found in names. These must be
# removed before the name can be parsed.
name_junk= ['Ms','Miss','Mrs','Mr','Master','Rev' ,'Fr' ,'Dr' ,'Atty' ,'Prof' \
    ,'Hon' ,'Pres','Gov' ,'Coach','Ofc' ,'Msgr' ,'Sr' ,'Br' ,'Supt','Rep' \
    ,'Sen' ,'Amb' ,'Treas','Sec' ,'Pvt' ,'Cpl' ,'Sgt' ,'Adm' ,'Maj' ,'Capt' \
    ,'Cmdr' ,'Lt' ,'Col' ,'Gen','esq','esquire','jr','jnr','sr','snr',\
    'ii','iii','iv']

# Regex that matches junck pieces in a name.
name_regex = re.compile('|'.join([r'(\b%s\b)'.encode('ASCII') % s.upper() for s in name_junk]))        





def strip_string(s):
    '''
    Collapse multiple whitespaces, and strip.
    @param s: a string.
    '''
    return re.sub(r'\s+',' ',s).strip()
    

def get_index(mylist, x):
    '''
    Find indices of all occurrences of C{x} in C{mylist}.
    '''
    return [i for i,y in enumerate(mylist) if y == x]
    


def splitname(name):
    '''
    Parse a name and return a three-tuple:
    C{(lastname, middlename, firstname)}. 
    When parsing, we first remove all junk as defined
    by L{name_junk} and L{name_regex}.
    '''
    s = name
    s1 = name_regex.sub('', s)
    s1 = re.sub(r'\.', ' ', s1)
    firstname, middlename, lastname = '', '', ''
    tree ='' 
    # If ',' exists, split based on that. Everything before
    # is last name.
    if s1.find(',') > 0:
        tree += '1'
        # Last name is everything left of the FIRST comma
        lastname, s_right = re.findall(r'([^\,]*),(.*)',s1)[0]

        #In case there are more commas:
        s_right = re.sub(r'\,',' ',s_right)
        s_right = strip_string(s_right)

        tokens = s_right.split(' ')
        
        lengths = [len(s) for s in tokens]
        length_max = max(lengths)
        if len(lengths) == 1:
            tree += '1'
            firstname = tokens[0]
            return lastname, middlename, firstname
        else:
            # multiple tokens on the right
            tree += '0'
            indices_1 = get_index(lengths,1)
            if len(indices_1) == 0:
                tree += '2'
                # Multiple tokens, all more than one letter
                # First token is first name, next is middle name
                firstname = tokens[0]
                tokens.remove(firstname)
                middlename = ' '.join(tokens)
                
                return lastname, middlename, firstname
            
            elif len(indices_1) == 1:
                tree += '1'
                # Only one single letter token.
                middlename = tokens[indices_1[0]]
                
                tokens.remove(middlename)
                firstname = ' '.join(tokens)
                #firstname = ' '.join(tokens)
                return lastname, middlename, firstname

            else:
                tree += '0'
                # multiple single-letter tokens
                # First one is middlename, 
                middlename = tokens[indices_1[0]]
                # If the first single letter token is not the
                # first RHS token, first name is all tokens uptp
                # the middle initial
                if indices_1[0] >= 1:
                    tree += '1'
                    firstname = ' '.join(tokens[0:indices_1[0]])
                else:
                    tree += '0'
                    # The first RHS token is single letter.
                    # What's the first name?
                    if length_max == 1:
                        tree += '1'
                        # If there are no multi-letter tokens,
                        # pick the second single letter one
                        # as first name
                        firstname = tokens[indices_1[1]]
                    else:
                        tree += '0'
                        # pick the first multi-letter token
                        # as first name
                        firstname = tokens[lengths.index(length_max)]
                    
                #firstname = ' '.join(tokens)
                return lastname, middlename, firstname
    else:
        # String doesn't contain comma
        # I examined a large number of records with NAME not
        # containing a comma. None were human names. So it doesn't
        # really matter how you parse those.
        tokens = s1.split(' ')
        lastname = tokens[0]
        firstname = ' '.join(tokens[1:])
        return lastname, middlename, firstname
            
                
            




def permute_inplace(X, Y):
    ''''
    permute the list C{X} inplace, according to C{Y}. C{Y} is a dictionary
    C{{c_index : t_index }} which means the value of C{X[c_index]} should
    end up in C{X[t_index]}.
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
            X[t_index], X[c_index] = X[c_index], X[t_index]
            Y[t_index], Y[c_index] = Y[c_index], Y[t_index]

        for key in death_row_keys:
            del Y[key]






def bad_identifier(identifier, type='employer'):
    '''
    Decide if the affiliation identifier is a "bad" or "low-information"
    identifier.
    '''
    if identifier == '': return True
    if type == 'employer':
        regex = r'\bNA\b|N\.A|employed|self|N\/A|\
                |information request|retired|teacher\b|scientist\b|\
                |applicable|not employed|none|\
                |homemaker|requested|executive|educator\b|\
                |attorney\b|physician|real estate|\
                |student\b|unemployed|professor\b|refused|docto\br|housewife|\
                |at home|president|best effort|consultant\b|\
                |email sent|letter sent|software engineer|CEO|founder|lawyer\b|\
                |instructor\b|chairman\b'
    elif type == 'occupation':
        regex = r'unknown|requested|retired|none|retire|retited|ret\b|declined|N.A\b|refused|NA\b|employed|self'
    else:
        print 'invalid identifier type'
        raise Exception("Identifier type must be either 'employer' or 'occupation'")
    if re.search(regex, identifier, flags=re.IGNORECASE):
        return True
    else:
        return False




def loadAffiliationNetwork(state, affiliation, percent=5, poststage1 = False):
    '''
    Loads the saved output of AffiliatoinAnalyzer from file: the affiliation network.
    It also adds a new attribute to the graph instance that contains a dictionary from
    affiliation identifier strings to the index of their corresponding vertex in the graph object.

    TODO: allow filtering based on value of an edge (or vertex) parameter
    '''

    def prune(G, field='significance', percent=5):
        '''
        Remove all but the top X percent of the edges with respect to the value of their field.
        '''
        deathrow = []
        n = len(G.es)
        threshold_index = n - n * percent / 100
        threshold_value = sorted(G.es[field])[threshold_index]

        for e in G.es:
            if e[field] < threshold_value:
                deathrow.append(e.index)
        G.delete_edges(deathrow)
        return G

    try:
        if affiliation == 'employer':
            if poststage1:
                filename = config.affiliation_poststage1_employer_file_template % state
            else:            
                filename = config.affiliation_employer_file_template % state
        elif affiliation == 'occupation':
            if poststage1:
                filename = config.affiliation_poststage1_occupation_file_template % state
            else:
                filename = config.affiliation_occupation_file_template % state
        else:
            raise Exception("Unable to load affiliation graphs. Affiliation must be 'occupation' or 'employer'")
#         filename = f = data_path + label + affiliation + '_graph.gml'
        print filename
        G = igraph.Graph.Read_GML(filename)

        try:
            G = prune(G, field='significance', percent=percent)
        except Exception, e:
            print e
            print "Error pruning the affiliation graph. Reloading the full graph."
            G = igraph.Graph.Read_GML(filename)

        dict_string_2_ind = {v['label']:v.index for v in G.vs}
        G.dict_string_2_ind = dict_string_2_ind
    except IOError:
        print "ERROR: Affiliation Network data not found."
        G = None

    # Not really necessary any more. I construct a {string: index} dictionary from the loaded Graph myself.
    # metadata = json.load(open(data_path + label + '-' + affiliation + '-metadata.json'))
    return G





def Log(message, msg_type="Error"):
    '''
    Log a message to the log file defined in config
    @param message: string message/
    @param msg_type: type of message. Can be "Error", "Warning", etc.
    '''
    filename = config.log_filename
    with open(filename, 'a') as f:
        now = time.strftime("%c")
        msg = config.log_message_template % (now, msg_type, message)
        f.write(msg)


def partition_list_of_graphs(mylist, num_partitions):
    '''
    Partition a list of graphs into subsets such that
    the total number of nodes in each subset is roughly
    equal.
    @param mylist: list of igraph.Graph instances
    @param num_partitions: desired number of partitions.
    @return: list where each element is a list of graphs.
    '''
    mylist.sort(key=lambda g:g.vcount())
    A = [[[], 0] for i in range(num_partitions)]

    while mylist:
        g = mylist.pop()
        A.sort(key=lambda subset:subset[1])
        A[0][0].append(g)
        A[0][1] += g.vcount()
#         print "Sizes of partitions: ", [subset[1] for subset in A]

    return [item[0] for item in A]


def prune_dict(mydict, condition_fcn, filename=''):
    '''
    conditionally remove items from dict without
    copying it in memory:
    write dict to file, clear the dict, then
    read back the data from file and insert into
    fresh dict based on condition.
    @param mydict: dictionary with tuple(int,int) keys and int values
    @param condition_fcn: condition function applied to values. Values
        for which condition_fcn(value) is True will be kept.
    @param filename: name of tmp file to use.
    '''
    if not filename:
        filename = config.dict_paths['tmp_path'] + "tmp-%d.txt" % random.randint(0, 100000)
    with open(filename, 'w') as f:
        for key, value in mydict.iteritems():
            if condition_fcn(value):
                f.write("%s|%s\n" % (str(key), str(value)))
    mydict.clear()
    mydict = {}
    with open(filename) as f:
        for line in f:
            l = line.strip()
            key, value = l.split("|")

            # Parse the key string into a tuple of two ints
            key = literal_eval(key)
            value = int(value)
            mydict[key] = value
    os.remove(filename)
    return mydict


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

    return 1. * len(set1.intersection(set2)) / len(set1.union(set2))


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








def argsort(seq):
    '''
    Generic argsort. returns the indices of the sorted sequence
    U{Source: <http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python>}
    '''
    return sorted(range(len(seq)), key=seq.__getitem__)

def shuffle_str(s):
    '''
    Shuffle string by converting to list, shuffling and converting back
    '''
    l = list(s)
    random.shuffle(l)
    return ''.join(l)


def sparse_vector(vec):
    '''
    This function converts a numpy 2d vector to a dictionary
    where the key is the index of the vector element and the
    value is the vector component corresponding to that coordinate.
    '''
    vec_sparse = {}
    for i in range(len(vec)):
        if vec[i] != 0:
            vec_sparse[i] = vec[i]
    return vec_sparse

def vec_norm(vec):
    '''
    this function computes the 2-norm of a sparse vector.
    '''
    total = 0
    for ind in vec:
        total += vec[ind] ** 2
    return np.sqrt(total)

def inner_product(vec_short, vec):
    '''
    This function computes the inner product of two sparse vectors.
    It is assumed that the first argument is the shorter of the two.
    The inned_product function takes advantage of the sparse representation of the vectors
    to significantly optimize the operation. The complexity of the inner product is independent
    of dim, it only depends on the average number of "non-zero" elements in the vectors.
    '''
    total = 0
    counter = 0
    for ind in vec_short:
        counter += 1
        total += vec_short[ind] * vec[ind]
    return total





def generate_rand_list_of_vectors(N, dim):
    '''
    Generate a random set of input vectors. For testing purposes.
    '''
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


def get_random_string(length):
    '''
    Return a random string of lengh length
    '''
    s = abcd
    return ''.join([random.choice(s) for i in range(length)])



