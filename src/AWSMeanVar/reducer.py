'''
Created on Feb 18, 2011

@author: mike-bowles
'''

import sys
from numpy import mat, mean, power

def read_input(file):
    for line in file:
        yield line.rstrip()
       
input = read_input(sys.stdin)#creates a list of input lines

#split input lines into separate items and store in list of lists

mapperOut = [instance.split() for instance in input]


#accumulate total number of samples, overall sum and overall sum sq
cumVal=0.0
cumSumSq=0.0
cumN=0.0
for instance in mapperOut:
    nj = float(instance[0])
    cumN = cumN + nj
    cumVal = cumVal + nj*float(instance[1])
    cumSumSq = cumSumSq + nj*float(instance[2])
    
#calculate means
 
mean = cumVal/cumN
meanSq = cumSumSq/cumN


#output size, mean, mean(square values)
print cumN, mean, meanSq  
print >> sys.stderr, "report: still alive" 


if __name__ == '__main__':
    pass