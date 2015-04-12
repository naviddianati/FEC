
from cPickle import load, dump
import time

def memory_spike(size=20,duration=5):
    '''
    Create a temporary object of given size and kep it in memory
    for the specified duration.
    '''
    
    mb =['a' for i in xrange(1000000)] 
    a = [ mb+[] for j in xrange(100 * size * 5/4)]
    time.sleep(duration)


if __name__ == "__main__":
    for i in range(4):
        time.sleep(10)
        memory_spike(10)
    
