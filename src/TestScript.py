'''
Created on Jul 25, 2014

@author: navid
'''

from copy import copy
import pickle

from main_general import *


def two_state_test():
    project1 = disambiguate_main('delaware', record_limit=(0, 5000))
    
    project1.D.tokenizer.tokenize_functions = None
    project1.D.tokenizer.normalize_functions = None
    project1.D.project = None
        
#     pickle.dumps(project1.D)
#     quit()
#     return project1



    project2 = disambiguate_main('oregon', record_limit=(0, 300))
    
    list_of_Ds = [project1.D, project2.D]
    
    D = Disambiguator.getCompoundDisambiguator(list_of_Ds)
    D.compute_LSH_hash(20)
    D.save_LSH_hash(batch_id='9999')
    D.compute_similarity(B1=40, m=20 , sigma1=None)
    
    #     project.save_data_textual(with_tokens=False, file_label="before")
    
    D.generate_identities()
    D.refine_identities()
        
    
    
    
    
    project1.D = D
    
    project1.save_data_textual(file_label="compound")









def worker(conn):
    data = conn.recv()
    proc_name = multiprocessing.current_process().name
    print proc_name, data
    
    list_results = []
    
    for state in data:
        try:
            project = disambiguate_main(state, record_limit=(0, 5000000))
#         project = generateAffiliationData(state)  

            
            # WOW! The following attributes contain pointers to instance methods, and they can't be pickled! So I just unset them!
            project.D.tokenizer.tokenize_functions = None
            project.D.tokenizer.normalize_functions = None
            project.D.project = None

            
            
            list_results.append(project) 
            print "="*70, "\n" + state + " done." + str(datetime.datetime.now()) + "\n" + "="*70 
            time.sleep(random.randint(1,10))

        except Exception as e:
            print "Could not disambiguate state ", state, ":   ",e
    f = open('tmp-'+proc_name,'w')
    pickle.dump(list_results,f)
    f.close()
#     for i,project in enumerate(list_results):
#         print i
#         s = pickle.dumps(project.D)
#     quit()
#    conn.send(list_results)    








if __name__ == "__main__":
    

    # Whether to run new state batches or read from existing output files.
    run_fresh_batches = True

    # Use custom list of states
    list_states = ['california', 'texas', 'marshallislands', 'palau', 'georgia', 'newjersey']
    list_states = ['delaware', 'maryland']
    list_states = ['virginia', 'maryland']
#    list_states = ['newyork', 'massachusetts']


    # Do the entire country
    # list_states = []

    list_jobs = []

    # if not specified,  load all states
    if not list_states:
        list_states = sorted(dict_state.values())


    set_states = set(list_states)

    N = len(list_states)

    # No more than 10 processes
    number_of_processes = min(N, 10)

    
    
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


    # Run fresh state-wide disambiguation batches. Alternatively, we can skip this and read from pickled files from previous runs. 
    if run_fresh_batches:        
        for id in dict_states:
            # queue = multiprocessing.Queue()
            conn_parent, conn_child = multiprocessing.Pipe()
            dict_conns[id] = (conn_parent, conn_child)        


            p = multiprocessing.Process(target=worker, name=str(id), args=(conn_child,))

            # set process as daemon. Let it run in the background
            # p.daemon = True

            list_jobs.append(p)
            time.sleep(1)
            p.start()
            conn_parent.send(dict_states[id])

               
        for p in list_jobs:
            p.join()
        
    
    # list containing the project objects returned by the processes
    list_results = []
    

    # Process the outputs
    for id in dict_states:
        #(conn_parent, conn_child) = dict_conns[id] 
        #result = conn_parent.recv()
        f = open("tmp-"+str(id))
        result =  pickle.load(f)
        list_results += result
        f.close()


    list_of_Ds = [project.D for project in list_results]
#     list_of_Ds = [D for D in list_results]

    print "Disambiguating the compound data..."
        
    try:
        D = Disambiguator.getCompoundDisambiguator(list_of_Ds)
    except Exception as e:
        print "ERROR: could not get compouns Disambiguator... ", e
    
    num_D_before = [len(D.set_of_persons), len(D.list_of_records)]
    
    D.compute_LSH_hash(20)
    D.save_LSH_hash(batch_id='9999')
    D.compute_similarity(B1=40, m=20 , sigma1=None)
    
    
    # D.generate_identities SHOULDN'T BE RUN FOR A COMPOUND D!
    D.refine_identities()
        
    project = copy(list_results[0])
#     
#     
    num_D_after = [len(D.set_of_persons), len(D.list_of_records)]


    project.D = D
#     

    project.save_data_textual(file_label="country")


    try:
        D.save_identities_to_db()
    except Exception as e:
        print "ERROR: ", e
    
    print num_D_before
    print num_D_after
    
    print ("="*70+"\n")*10
    
    D.print_set_of_persons(D.get_set_of_persons_multistate(), 'Persons with multiple states in timeline')

        

 
