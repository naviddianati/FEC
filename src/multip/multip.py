'''
This is a test of multiprocessing in Python where processes are spawned inside 
an instance method and then return results to the parent method.
The number of processes can be changed and the computation is timed.
'''


from cmath import exp
from multiprocessing import *
from time import time,sleep

# Accessible to child processes
globaldata = 127

def worker(p_data):
    pid = p_data['pid']
    print 'worker %d started.' % pid
    data = p_data['data']
    queue = p_data['queue']
    
    doSomething(data)
    queue.put('Done with process '+ str(pid))
    
    pass



def doSomething(data):
    #print data ,'----'
    #return
    print id(data[0])
    sleep(20)

def actionDummy(data):
    print "worker working started ", data, globaldata, " ",  thing.N
    for x in thing.data:
        pass
    print "Sleeping"
    sleep(10)



class Thing:
    def __init__(self):
        self.N = 1000000*100
        #self.data = range(self.N)
        self.data = [{} for i in xrange(self.N)]
        self.num_procs = 10
    
    
    
    def chunkit(self,data,i,num_chunks,B):
        ''' 
        Return a chunk of the data (list)
        parameters:
            data    : a list
            i    : return the ith chunk
            num_chunks: total number of chunks
            B: 
            '''
        pass
        N = len(data)
        chunk_size = N / num_chunks + 1
        output = data[chunk_size * i: chunk_size * (i+1)]
        return output



    def getchunk(self,data,i,num_chunks,B):
        ''' 
        split data into chunks, and delete each chunk from data as it's sliced off
        parameters:
            data    : a list
            i    : return the ith chunk
            num_chunks: total number of chunks
            B: 
            '''
        pass
        N = len(data)
        chunk_size = N / num_chunks + 1
        
        output = []
        for i in range(num_chunks):
            output.append(data[:chunk_size] )
            del data[:chunk_size]
        return output
    

    def run2(self):
        # This data is invisible to child processes
        insidedata = 394

        #pool = Pool(processes = 10)
        #pool.map(actionDummy,range(10))

        print "Spawning children in 5 seconds"
        sleep(5)
        list_procs = [Process(target = actionDummy, args=[[]]) for i in range(10)]
        for p in list_procs: p.start()
        for p in list_procs: p.join()

        #p_data['data'] = self.chunkit(self.data, i,self.num_procs,0)
        
    
    def run1(self):
        list_procs = []
        list_queues = []
        print "spawning children in 5 seconds"
        sleep(5)     
        # Create processes
    
        list_data = [self.getchunk(self.data,i,self.num_procs,0) for i in range(self.num_procs)]
        for i in range(self.num_procs):

            p_data = {}
            p_data['pid'] = i
            q = Queue()
            list_queues.append(q)
            p_data['queue'] = q            
            p_data['data'] = list_data[i]
                
            
            p = Process(target=worker, args = (p_data,))
            list_procs.append(p)
            p.start()
            print self.data        

        for i,p in enumerate(list_procs):
            result = list_queues[i].get() 
            print result   
        
        for p in list_procs:
            p.join()            
        


print __name__       


if __name__=="__main__":
    
    # This data is accessible to child processes spawned inside thing
    maindata = 666

    thing = Thing()
    
    t1 = time()
    thing.run1()
    t2 = time()
    
    print t2-t1

 
