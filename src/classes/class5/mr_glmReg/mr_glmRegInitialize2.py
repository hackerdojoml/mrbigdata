'''
Created on May 3, 2011

@author: mike-bowles
'''
from mrjob.job import MRJob

from numpy import mat, zeros, shape, random, array, zeros_like, dot, linalg
from random import sample
import json
from math import pi, sqrt, exp, pow

def listSum(x,y):
    n = len(x)
    rtn = [0.0]*n
    for i in range(n):
        rtn[i] = x[i] + y[i]
    return rtn

def listSq(x):
    n = len(x)
    rtn = [0.0]*n
    for i in range(n):
        rtn[i] = x[i]*x[i]
    return rtn
        
def listMult(x,a):
    n = len(x)
    rtn = [0.0]*n
    for i in range(n):
        rtn[i] = x[i]*a
    return rtn

def listDiff(x,y):
    n = len(x)
    rtn = [0.0]*n
    for i in range(n):
        rtn[i] = x[i] - y[i]
    return rtn

class MrGlmRegInit2(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MrGlmRegInit2, self).__init__(*args, **kwargs)
        fullPath = self.options.pathName + 'scaleFactors.txt'
        fileIn = open(fullPath)
        inputJson = fileIn.read()
        fileIn.close()
        inputList = json.loads(inputJson)
        self.meanY = inputList[0]
        self.meanX = inputList[1]
        self.stdDevX = inputList[2]
        self.sum = []
        self.numMappers = 1     #number of mappers
        self.count = 0
        
                                                 
    def configure_options(self):
        super(MrGlmRegInit2, self).configure_options()
        self.add_passthrough_option(
            '--alpha', dest='alpha', default=0.2, type='float',
            help='alpha: elasticNet parameter (l1 vs l2 penalty)')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="//home//mike-bowles//pyWorkspace//mapReducers//src//mr_glmReg//", type='str',
            help='pathName: pathname where scale-factors and coefficient trajectories are stored')
        
    def mapper(self, key, yxjIn):
        #yxjIn = [y,[x1,x2, ...]]
        #form partial sums of x and x^2 (component-wise square) - return these
        #along with number of instances handled
        
        #load input y,x and standardize them
        input = json.loads(yxjIn)
        y = input[0]
        x = input[1]
        yscale = (y - self.meanY)
        xscale = listDiff(x,self.meanX)
        for i in range(len(xscale)):
            xscale[i] = xscale[i]/self.stdDevX[i]
        if self.count == 0:
            #initialize "sum" as the list whose elements are the elements of the 
            #list "x" each one multiplied by the scalar "y"
            self.sum = listMult(xscale,yscale)
            self.count += 1
        else:
            #increment the list "sum" by the list "x" each element multiplied by "y"
            temp = self.sum
            self.sum = listSum(temp, listMult(xscale,yscale))
            self.count += 1
        
        if False: yield (1,yxjIn)
    
    def mapper_final(self):
        out = [self.count, self.meanY, self.sum]
        jOut = json.dumps(out)
        yield 1 , jOut
        
    def reducer(self, key, xjIn):        
        #calc grand totals from all mappers
        first = True
        for xj in xjIn:
            sumStats = json.loads(xj)
            if first:
                count = sumStats[0]
                sum = sumStats[2]
                meanY = sumStats[1]
                first = False
            else:
                count += sumStats[0]
                temp = sum
                x = sumStats[2]
                sum = listSum(temp,x)
                
        #at this point we've got list-sum of x's times scalar y's
        #extract the starting value of lambda and write that along with beta = [0.0, ... ]
        #corresponding starting weight vector.  
        
        const = 1.0/count
        temp = sum
        sum = listMult(temp,const)
        #determine lambda large enough that all betas are zero
        max = 0.0
        for i in range(len(sum)):
            comp = abs(sum[i])
            if comp > max:
                max = comp
            
        lambdaStart = max/self.options.alpha
        betaStart = [0.0]*len(sum)
        beta0Start = meanY
        
        '''
        jDebug = json.dumps([cent2,mean.tolist(),cov.tolist(),covInv.tolist(),cov_1])    
        debugPath = self.options.pathName + 'debug.txt'
        fileOut = open(debugPath,'w')
        fileOut.write(jDebug)
        fileOut.close()        
        '''
                
        #form output object
        outputList = [lambdaStart, beta0Start, betaStart]
            
        jsonOut  = json.dumps(outputList)
        
        #write new parameters to file
        fullPath = self.options.pathName + 'lambdaBeta.txt'
        fileOut = open(fullPath,'w')
        fileOut.write(jsonOut)
        fileOut.close()
        fullPath = self.options.pathName + 'lambdaPath.txt'
        fileOut = open(fullPath,'w')
        fileOut.write(jsonOut)
        fileOut.close()
        if False: yield 1,2

if __name__ == '__main__':
    MrGlmRegInit2.run()