#! /usr/bin/python



def profile_memory(n = 10, delay=1, outfile="memlog.csv"):
    '''
    Save profiles of the memory usage as indicated by /proc/meminfo
    at several intervals and export them to a csv file.
    @param n: number of samples
    @param delay: delay in seconds between samples
    @param outfile: output file name    
    '''
    import pandas as pd
    import time
    def get_memory_profile():
        '''
        Read /proc/meminfo and return all information as
        a dictionary. values are ints in kilobytes.
        '''
        data = pd.read_csv("/proc/meminfo",sep=":",names=["field","value"])
        dict_data = {row["field"] : int(row["value"].strip().replace(" kB","")) for  i,row in data.iterrows()}
        dict_data['time'] = time.time()
        return dict_data
    
    f = open(outfile,'w')
    profile = pd.DataFrame(get_memory_profile(),index=[0])
    profile.to_csv(f,index=False,header=True)
    f.close()


    for i in xrange(n):

        profile = pd.DataFrame(get_memory_profile(),index=[0])
        f = open(outfile,'a')
        profile.to_csv(f,index=False,header=False)
        f.close()

        time.sleep(delay)




if __name__=="__main__":
    profile_memory(n = 100000, delay = 1)
