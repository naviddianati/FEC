from math import sqrt,acos,pi

def get_prob(spec):
    m,n1,n2=spec
    cosangle = m/sqrt(m+n1)/sqrt(m+n2)
    prob = acos(cosangle)/pi
    return prob
    

all = []



all.append([1,1,1])
all.append([1,2,2])
all.append([1,1,2])

all.append([2,0,0])
all.append([2,1,0])
all.append([2,2,0])
all.append([2,1,1])
all.append([2,1,2])

all.append([3,1,0])
all.append([3,2,0])
all.append([3,1,1])
all.append([3,1,2])

all.append([1,2,1])

for spec in all:
    print spec,' ',get_prob(spec)