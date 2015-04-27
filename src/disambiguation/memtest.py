from multiprocessing import Pool
import time 
from cPickle import loads, dumps
import sys

def worker(data):
    time.sleep(20)


if __name__ == "__main__":
    numprocs = 10
    pool = Pool(numprocs)
    a = 'a' *10000000
    b = [a+'' for i in xrange(10000)]

    time.sleep(10)
    data1 = [b[:] for i in range(numprocs)]
    data2 = [data1[:]] + ['1' for i in range(numprocs-1)]
    data3 = [['1'] for i in range(numprocs)]

    data = data1
    #data = data2
    #data = data3
    
    print sum([sys.getsizeof(dumps(d)) for d in data1])
    print sum([sys.getsizeof(dumps(d)) for d in data2])
    quit()
 
    result = pool.map(worker,data)



