'''
This script disambiguates all states, one at a time, and using 
all available cores in parallel.
'''

from copy import copy
import pickle
from main import *
from core.states import get_state_order




def process_state(state, num_procs):
    '''
    Run the disambiguation for the given state and record
    the resulting identities into the database. 
    '''
    project = disambiguate_main(state, record_limit=(0, 5000000), num_procs = num_procs)
    
    try:
        project.D.save_identities_to_db()
    except Exception as e:
        print "ERROR: ", e
        raise 

    #except Exception as e:
    #    print "Could not disambiguate state ", state, ":   ", e





if __name__ == "__main__":

    dict_state_order = get_state_order()


    # Total number of processes we can use in different stages. This specifies  both the number
    # of processes used for parallel processing of the different states, and the number of processes
    # used to distribute the combined processing of all states together.
    num_procs = 18


    # Use custom list of states
    #list_states = ['virginia', 'maryland']
    #list_states = ['california', 'texas', 'marshallislands', 'palau', 'georgia', 'newjersey']
    #list_states = ['newjersey']
    list_states = ['newyork']
    
    # Do the entire country
    # list_states = []

    list_jobs = []

    # if not specified,  load all states
    if not list_states:
        list_states = sorted(dict_state.values())


    list_states.sort(key=lambda state: dict_state_order[state])
    print list_states


    for state in list_states:
        process_state(state, num_procs = num_procs)


