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
input = [float(line) for line in input] #overwrite with floats
numInputs = len(input)
input = mat(input)
sqInput = power(input,2)

#output size, mean, mean(square values)
print numInputs, mean(input), mean(sqInput)#calc mean of columns
print >> sys.stderr, "report: still alive" 


if __name__ == '__main__':
    pass