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
        chunk =  l[int(i*size) - n_removed : int((i+1) * size) - n_removed]
        list_chunks.append(chunk)
        del l[int(i*size) - n_removed : int((i+1) * size) - n_removed]
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
    return [l[int(i*size):int((i+1) * size)] for i in range(n)]

        

