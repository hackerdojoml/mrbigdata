'''
Created on May 3, 2011

@author: mike-bowles
'''
from mrjob.job import MRJob

from numpy import mat, zeros, shape, random, array, zeros_like, dot, linalg
from random import sample
import json
from math import pi, sqrt, exp, pow, fabs

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

def listDot(x,y):
    n = len(x)
    rtn = 0.0
    for i in range(n):
        rtn += x[i]*y[i]
    return rtn

def S(z,gamma):
    if gamma >= fabs(z):
        return 0.0
    if z > 0.0:
        return z - gamma
    else:
        return z + gamma

class MrGlmRegIter(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MrGlmRegIter, self).__init__(*args, **kwargs)
        fullPath = self.options.pathName + 'scaleFactors.txt'
        fileIn = open(fullPath)
        inputJson = fileIn.read()
        fileIn.close()
        inputList = json.loads(inputJson)
        self.meanY = inputList[0]
        self.meanX = inputList[1]
        self.stdDevX = inputList[2]
        fullPath = self.options.pathName + 'lambdaBeta.txt'
        fileIn = open(fullPath)
        inputJson = fileIn.read()
        fileIn.close()
        inputList = json.loads(inputJson)
        self.lam = inputList[0]*self.options.lambdaMult
        self.b0 = inputList[1]
        self.beta = inputList[2]
        
        self.sum = []
        self.numMappers = 1     #number of mappers
        self.count = 0
        
                                                 
    def configure_options(self):
        super(MrGlmRegIter, self).configure_options()
        self.add_passthrough_option(
            '--alpha', dest='alpha', default=1.2, type='float',
            help='alpha: elasticNet parameter (l1 vs l2 penalty)')
        self.add_passthrough_option(
            '--lambda', dest='lambdaMult', default=0.97, type='float',
            help='lambda: weight on coefficient penalty')
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="//home//mike-bowles//pyWorkspace//mapReducers//src//mr_glmReg//", type='str',
            help='pathName: pathname where scale-factors and coefficient trajectories are stored')
        
    def mapper(self, key, yxjIn):
        #yxjIn = [y,[x1,x2, ...]]
        #form sums needed for gradient calc
        #along with number of instances handled
        
        #unpack and normalize y and x
        input = json.loads(yxjIn)
        y = input[0]
        x = input[1]
        yscale = (y - self.meanY)
        xscale = listDiff(x,self.meanX)
        for i in range(len(xscale)):
            xscale[i] = xscale[i]/self.stdDevX[i]
        
        #form residual (y - b0 - x*beta)
        r = yscale - listDot(xscale,self.beta)        
        
        
        if self.count == 0:
            #initialize "sum" as the list whose elements are the elements of the 
            #list "x" each one multiplied by the scalar "r" - residual
            self.sum = listMult(xscale,r)
            self.count += 1
        else:
            #increment the list "sum" by the list "x" each element multiplied by "y"
            temp = self.sum
            self.sum = listSum(temp, listMult(xscale,r))
            self.count += 1
        
        if False: yield (1,yxjIn)
    
    def mapper_final(self):
        out = [self.count, self.b0, self.sum]
        jOut = json.dumps(out)
        yield 1 , jOut
        
    def reducer(self, key, xjIn):        
        #calc grand totals from all mappers
        first = True
        for xj in xjIn:
            sumStats = json.loads(xj)
            if first:
                count = sumStats[0]
                meanY = sumStats[1]
                sum = sumStats[2]
                first = False
            else:
                count += sumStats[0]
                temp = sum
                temp2 = sumStats[2]
                sum = listSum(temp,temp2)
                
                
        const = 1.0/float(count)
        temp = sum
        sum = listMult(temp,const)
        #this gives us the 1/N Sum(xij*ri) term from eq 8 in friedman's paper
        
        dBeta = listSum(sum, self.beta)
        newBeta = [0.0]*len(sum)
        for i in range(len(sum)):
            newBeta[i] = S(dBeta[i],self.lam*self.options.alpha)/(1+self.lam*(1 - self.options.alpha))
            
               
        '''
        jDebug = json.dumps([cent2,mean.tolist(),cov.tolist(),covInv.tolist(),cov_1])    
        debugPath = self.options.pathName + 'debug.txt'
        fileOut = open(debugPath,'w')
        fileOut.write(jDebug)
        fileOut.close()        
        '''
                
        #form output object
        outputList = [self.lam, meanY, newBeta]
            
        jsonOut  = json.dumps(outputList)
        
        #write new parameters to file for next iteration
        fullPath = self.options.pathName + 'lambdaBeta.txt'
        fileOut = open(fullPath,'w')
        fileOut.write(jsonOut)
        fileOut.close()
        #append parameters to file containing entire path history
        fullPath = self.options.pathName + 'lambdaPath.txt'
        fileOut = open(fullPath,'a')
        fileOut.write(jsonOut)
        fileOut.close()
        if False: yield 1,2

if __name__ == '__main__':
    MrGlmRegIter.run()