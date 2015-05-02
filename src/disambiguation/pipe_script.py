'''
Created on Jul 25, 2014

@author: navid
'''

from copy import copy
import pickle
import math
import sys

from main_general import *



def get_large_obj(size):
    ''' size is in megabytes '''
    a = ''
    for i in xrange(1000*1024):
        a += 'a'
        x = math.sqrt(11)
    print sys.getsizeof(a)
    return a*size
    


def worker_disambiguate_states(conn):
    data = conn.recv()
    proc_name = multiprocessing.current_process().name
    print proc_name, data
    
    list_results = []
    
    for state in data:
        try:
            # 2 Gb
            project = get_large_obj(1000)
            print sys.getsizeof(project)

            list_results.append(project) 
            print "="*70, "\n" + state + " done." + str(datetime.datetime.now()) + "\n" + "="*70 
            #time.sleep(random.randint(1,10))

        except Exception as e:
            print "Could not disambiguate state ", state, ":   ",e
    f = open('tmp-'+proc_name,'w')
    pickle.dump(list_results,f)
    f.close()
    #conn.send(list_results)    








if __name__ == "__main__":
    

    # Use custom list of states
    list_states = ['california', 'texas', 'marshallislands', 'palau', 'georgia', 'newjersey']
    list_states = ['delaware', 'maryland']
#    list_states = ['newyork', 'massachusetts']


    # Do the entire country
    list_states = []

    list_jobs = []

    # if not specified,  load all states
    if not list_states:
        list_states = sorted(dict_state.values())


    set_states = set(list_states)

    N = len(list_states)

    # No more than 10 processes
    number_of_processes = min(N, 5)

    
    
    dict_states = {}
    
    dict_conns = {}


    proc_id = 0
    while set_states:
        if proc_id not in dict_states: dict_states[proc_id] = set()
        dict_states[proc_id].add(set_states.pop())
        proc_id += 1
        proc_id = proc_id % number_of_processes

    for id in dict_states:
        print id, dict_states[id]

        
    for id in dict_states:
        # queue = multiprocessing.Queue()
        conn_parent, conn_child = multiprocessing.Pipe()
        dict_conns[id] = (conn_parent, conn_child)        


        p = multiprocessing.Process(target=worker_disambiguate_states, name=str(id), args=(conn_child,))

        # set process as daemon. Let it run in the background
        # p.daemon = True

        list_jobs.append(p)
        #time.sleep(1)
        p.start()
        conn_parent.send(dict_states[id])

    
    # list containing the project objects returned by the processes
    list_results = []
    
    
    # Process the outputs
    for id in dict_states:
        (conn_parent, conn_child) = dict_conns[id] 
        #result = conn_parent.recv()
        #list_results += result
        
    for p in list_jobs:
        p.join()

    for id in dict_states:
        f = open("tmp-"+str(id))
        result =  pickle.load(f)
        list_results += result
        f.close()

    #print list_of_Ds 
    print "Received and combined all data" 
    time.sleep(100)     
#     
       

 
