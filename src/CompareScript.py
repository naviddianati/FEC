'''
Created on Jul 25, 2014

@author: navid
'''

from copy import copy
import pickle

from main_general import *





def worker(conn):
    
    # data is a subset of list_trial_ids
    data = conn.recv()
    proc_name = multiprocessing.current_process().name
    print proc_name, data
    

    # This is a list or tuples. Each tuple is (process_id , dict_identities)
    list_results = []
    
    

    for id in data:
        try:
            print "processing task with id = ", id
            project = disambiguate_main(state, record_limit=(0, 5000000),method_id = id)
            D = project.D

            # To be returned to parent process. Dictionary {record_id: identity}
            dict_identities = {}

            for id_pair in D.generator_identity_list():
                record_id,identity = id_pair
                dict_identities[record_id] = identity


            list_results.append((id,dict_identities))
            print "="*70, "\n id " + id  + " done." + str(datetime.datetime.now()) + "\n" + "="*70 

        except Exception as e:
            print "could not complete task  ", id, ":   ",e
    f = open('tmp-compare-'+proc_name,'w')
    pickle.dump(list_results,f)
    f.close()
#     for i,project in enumerate(list_results):
#         print i
#         s = pickle.dumps(project.D)
#     quit()
#    conn.send(list_results)    








if __name__ == "__main__":
    

    #list_states = ['delaware', 'maryland']
    state = "ohio"
    list_jobs = []


    #set_states = set(list_states)

    list_trial_ids = ["thorough","1","2","3","4","5","6","7","8"]

    #list_trial_ids = [str(i) for i in range(20)]

    set_trial_ids = set(list_trial_ids)
    
    N = len(list_trial_ids)

    # No more than 10 processes
    number_of_processes = min(N, 10)

    
    
    dict_trials = {} 
    dict_conns = {}


    proc_id = 1
    while set_trial_ids:
        if proc_id not in dict_trials: dict_trials[proc_id] = set()
        dict_trials[proc_id].add(set_trial_ids.pop())
        proc_id += 1
        proc_id = proc_id % number_of_processes

    for id in dict_trials:
        print id, dict_trials[id]

        
    for id in dict_trials:
        # queue = multiprocessing.Queue()
        conn_parent, conn_child = multiprocessing.Pipe()
        dict_conns[id] = (conn_parent, conn_child)        


        p = multiprocessing.Process(target=worker, name=str(id), args=(conn_child,))

        # set process as daemon. Let it run in the background
        # p.daemon = True

        list_jobs.append(p)
#        time.sleep(1)
        p.start()
        conn_parent.send(dict_trials[id])

    
    # list containing the project objects returned by the processes
    list_results = []
    
    
           
    for p in list_jobs:
        p.join()
    

    print "All processes returned"
   
    # Process the outputs
    for id in dict_trials:
        #(conn_parent, conn_child) = dict_conns[id] 
        #result = conn_parent.recv()
        f = open("tmp-compare-methods-ohio-"+str(id))
        result =  pickle.load(f)
        list_results += result
        print "Data from process %s loaded"% id
        f.close()

    #for id,dict_identities in  list_results:
    #    print dict_identities

        
    
#    try:
#        D.save_identities_to_db()
#    except Exception as e:
#        print "ERROR: ", e
    
    print ("="*70+"\n")*10
    

        

 
