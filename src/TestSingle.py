#! /usr/bin/python


'''
Created on Jul 29, 2014

@author: navid
'''
'''
Created on Jul 25, 2014

@author: navid
'''

from copy import copy
import pickle
from sys import argv

from Database import DatabaseManager
from main_general import *


    






if __name__ == "__main__":
    
    
    list_states = ['ohio','newyork','massachusetts','california','texas']
    id = argv[1]
    state = list_states[int(id)]
    project1 = disambiguate_main(state, record_limit=(0, 5000000), logstats = True)
    
    
#     D.save_identities_to_db()
    
#     for person in D.set_of_persons:
#         print person.neighbors

    
    
 
