#! /usr/bin/python


from numpy import *
import re


def get_likelihood(h):
    N = len(h)
    p = 1.0
    for i in range(N-1):
        p *= U[h[i],h[i+1]]
    return p
        

def get_score(h):
    N = float(len(h))
    h = ''.join(h)
    groups = re.findall(r'1+|0+',h)
    score = prod([len(g)/N for g in groups])
    return score


def coinflip(p):
    if random.random()<p:
        return True
    else:
        return False

def flip_and_invert(s):
    return ''.join([str(int(not(int(x)))) for x in s[::-1]])
    
def generate_sequence(N):
    x = 0
    s = str(x)
    no_of_transitions = 0
    no_trans = [0 for i in range(N)]
    for i in range(N-1):
        x_old = x
        x = x if coinflip(T[x,x]) else int(not(x))
        if x!=x_old: no_of_transitions += 1
        s += str(x)
    score1 = get_likelihood(s)
    score2 = get_likelihood(flip_and_invert(s))
    score = score1/get_likelihood(no_trans)
    
    #~ print s,'   ',flip_and_invert(s),'      ', no_of_transitions, "  %f     %4.3f  %4.3f" % (score,score1,score2)
    #~ print "%s %s %d %f %0.3f" % (s,flip_and_invert(s),no_of_transitions,score,get_score(s)) 
    return s,get_score(s)
    
def generate_random_sequence(N):
    s = ''.join(['1' if random.random()<0.5 else '0' for i in range(N)])
    return s,get_score(s)

epsilon = 0.05
p0 = 0.7
p1 = 0.80
T = array([[p0,1-p0],[1-p1,p1]])


epsilon = 0.04
p0 = 0.9
p1 = 0.97
U = array([[p0,1-p0],[epsilon,p1]])

#~ print flip_and_invert([1,0,0,0,0,1,1])
#~ quit()



list_all = []

num_strings = 30


#~ for i in range(num_strings):
    #~ s,score=generate_sequence(5)
    #~ list_all.append([s,score])
    
i = 0
while i < num_strings:
    s,score=generate_sequence(10)
    #~ s,score=generate_random_sequence(10)

    if score == 1: continue
    i += 1
    list_all.append([s,score])
    

list_all = sorted(list_all, key=lambda x:x[1],reverse = True)

for i in range(num_strings):
    s,score = list_all[i]
    print "%d %0.14f 0.5 %s" % (i,get_score(s),s) 

   


h = [1,1,1,1,1,1]




