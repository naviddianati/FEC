from main_general import *

def worker_generate_affiliation(conn):
    ''' The worker function that performs affiliation computation for a list of 
    (state,affiliaiton) tuples.'''

    data = conn.recv()
    #print data
    proc_name = multiprocessing.current_process().name
    

    
    print "_"*80+"\n"+"process started " , proc_name, data,"\n","_"*80+"\n"
    
    for item in data:
        state,affiliation = item
        # This exception handling is important. In case one job fails, we need to move on
        # to the next!
        try:
            print "--------------",state,affiliation
            generateAffiliationData(state,affiliation = affiliation)   
        except Exception as e:
            print 'ERROR ',e
        print "="*70, "\n" + state + " done." + str(datetime.datetime.now()) + "\n" + "="*70 




def get_todo_affiliation_jobs():
    '''returns a list of tuples: [(state,affiliation)] of states that need
        their affiliation network computed.'''

    data_path = os.path.expanduser('~/data/FEC/')

    list_todo = []

    list_states = sorted(dict_state.values())
    for state in list_states:
        file1 =  glob.glob(data_path+state+"*employer_graph.gml")
        file2 =  glob.glob(data_path+state+"*occupation_graph.gml")

        if (not file1) and (not file2):
            list_todo.append((state,None))
        else:
            if not file1:
                print "generateAffiliationData(%s,affiliation='employer')" % state
                list_todo.append((state,'employer'))
            if not file2:
                print "generateAffiliationData(%s,affiliation='occupation')" % state
                list_todo.append((state,'occupation'))

    return list_todo







def schedule_jobs(jobs):
    '''jobs is a list of tuples: [(state,affiliation)]'''
    if not jobs: return

    N =  len(jobs)

    # No more than 10 processes
    number_of_processes = min(N,8)

    dict_states = {}
    
    dict_conns = {}

    list_procs = []
    
    proc_id = 0
    while jobs:
        if proc_id not in dict_states: dict_states[proc_id] = set()
        item = jobs.pop()
        dict_states[proc_id].add(item)
        proc_id +=1
        proc_id = proc_id %number_of_processes

    for id in dict_states:
        print id, dict_states[id]

    for id in dict_states:
        # queue = multiprocessing.Queue()
        conn_parent, conn_child = multiprocessing.Pipe()
        dict_conns[id] = (conn_parent, conn_child)        

        p = multiprocessing.Process(target=worker_generate_affiliation, name=str(id), args=(conn_child,))

        # set process as daemon. Let it run in the background
        # p.daemon = True

        list_procs.append(p)
        p.start()
        conn_parent.send(dict_states[id])
    
    for p in list_procs:
        p.join()
    

       
        




if __name__ == "__main__":

    jobs = get_todo_affiliation_jobs()
    #jobs = [('indiana','employer')]
    #jobs = [('newyork', 'occupation')]
    #print jobs
    #quit()
    schedule_jobs(jobs)
    quit()



