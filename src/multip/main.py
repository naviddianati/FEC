'''
This is a test of multiprocessing in Python where processes are spawned inside 
an instance method and then return results to the parent method.
The number of processes can be changed and the computation is timed.
'''


from time import time,sleep
import multip



if __name__=="__main__":
    

    thing = multip.Thing()

    # This ensures that thing is visible from within the multip module as a golbal variable
    # Without this, 
    #multip.thing = thing    


    t1 = time()
    thing.run1()
    t2 = time()
    
    print t2-t1

        
