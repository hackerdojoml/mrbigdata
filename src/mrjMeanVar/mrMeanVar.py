'''
Created on Mar 1, 2011

@author: mike-bowles
'''
from mrjob.job import MRJob
from math import sqrt
import json


class mrMeanVar(MRJob):
    #DEFAULT_PROTOCOL = 'raw_value'
    #DEFAULT_PROTOCOL = 'json_value'
    DEFAULT_PROTOCOL = 'json'
    
    '''
    #default for DEFAULT_PROTOCOL
    def mapper(self, key, line):
        
        txt = line.rstrip()
        num = float(txt)
        var = [num,num*num]
        yield 1,var
    
    
    '''  
        
    '''
    #works with either json or json_value for default protocol
    def mapper(self, key, dataJson):
        
        data = json.loads(dataJson)
        
        for val in data:
            num = float(val)
            var = [num,num*num]
            yield 1,var
    
    '''
    
    
    '''this mapper also works with json for default protocol, but has coded 
    the input differently from the mapper above.  the mapper above reads in the 
    whole list of input values as a single python list object.  this mapper reads
    each datum separately.'''
    def mapper(self, key, line):
        
        num = json.loads(line)
        var = [num,num*num]
        yield 1,var

  
    def reducer(self, n, vars):
        N = 0.0
        sum = 0.0
        sumsq = 0.0
        for x in vars:
            N += 1
            sum += x[0]
            sumsq += x[1]
        mean = sum/N
        sd = sqrt(sumsq/N - mean*mean)
        results = [mean,sd]
        yield 1,results

if __name__ == '__main__':
    mrMeanVar.run()
