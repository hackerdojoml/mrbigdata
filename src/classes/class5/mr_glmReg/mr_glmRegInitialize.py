'''
Created on Apr 18, 2011

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


class MrGlmRegInit1(MRJob):
    DEFAULT_PROTOCOL = 'json'
    
    def __init__(self, *args, **kwargs):
        super(MrGlmRegInit1, self).__init__(*args, **kwargs)
        self.sumy = []
        self.sumx = []
        self.sumSqx = []
        self.numMappers = 1     #number of mappers
        self.count = 0
        
                                                 
    def configure_options(self):
        super(MrGlmRegInit1, self).configure_options()
        self.add_passthrough_option(
            '--pathName', dest='pathName', default="//home//mike-bowles//pyWorkspace//mapReducers//src//mr_glmReg//", type='str',
            help='pathName: pathname where scale-factors and coefficient trajectories are stored')
        
    def mapper(self, key, yxjIn):
        #yxjIn = [y,[x1,x2, ...]]
        #form partial sums of x and x^2 (component-wise square) - return these
        #along with number of instances handled
        yxList = json.loads(yxjIn)
        y = yxList[0]
        x = yxList[1]
        if self.count == 0:            
            self.sumy = y
            self.sumx = x
            self.sumSqx = listSq(x)
            self.count += 1
        else:
            self.sumy += y
            self.sumx = listSum(self.sumx, x)
            self.sumSqx = listSum(listSq(x),self.sumSqx)
            self.count += 1
        
        if False: yield (1,yxjIn)
    
    def mapper_final(self):
        out = [self.count,self.sumy,self.sumx, self.sumSqx]
        jOut = json.dumps(out)
        yield 1 , jOut
        
    def reducer(self, key, xjIn):        
        #calc grand totals from all mappers
        first = True
        for xj in xjIn:
            sumStats = json.loads(xj)
            if first:
                count = sumStats[0]
                sumy = sumStats[1]
                sumx = sumStats[2]
                sumSqx = sumStats[3]
                
                first = False
            else:
                count += sumStats[0]
                sumy += sumStats[1]
                temp = sumStats[2]
                sumx = listSum(sumx,temp)
                temp = sumStats[3]
                sumSqx = listSum(sumSqx,temp) 
        #at this point we've got component-wise sum of x's and y's
        #form scaling factors and write to file
        a = 1.0/float(count)
        meany = sumy*a
        meanx = listMult(sumx, a)
        var = listDiff( listMult(sumSqx,a), listSq(meanx) )
        sd = [0.0] * len(var)
        for i in range(len(var)):
            sd[i] = sqrt(var[i])
                   
      
        '''
        jDebug = json.dumps([cent2,mean.tolist(),cov.tolist(),covInv.tolist(),cov_1])    
        debugPath = self.options.pathName + 'debug.txt'
        fileOut = open(debugPath,'w')
        fileOut.write(jDebug)
        fileOut.close()        
        '''
                
        #form output object
        outputList = [meany, meanx, sd]
            
        jsonOut  = json.dumps(outputList)
        
        #write new parameters to file
        fullPath = self.options.pathName + 'scaleFactors.txt'
        fileOut = open(fullPath,'w')
        fileOut.write(jsonOut)
        fileOut.close()
        if False: yield 1,2

if __name__ == '__main__':
    MrGlmRegInit1.run()