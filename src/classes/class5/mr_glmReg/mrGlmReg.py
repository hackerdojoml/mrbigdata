'''
Created on Apr 18, 2011

@author: mike-bowles
'''
from mr_glmRegInitialize import MrGlmRegInit1
from mr_glmRegInitialize2 import MrGlmRegInit2
from mr_glmRegIterate import MrGlmRegIter
import json
from math import sqrt

def dist(x,y):
    #euclidean distance between two lists    
    sum = 0.0
    for i in range(len(x)):
        temp = x[i] - y[i]
        sum += temp * temp
    return sqrt(sum)


'''
EM for gaussian mixture model.  
sequence of events
1.  initialize with modified kmeans initializer
2.  generate 1/0 initial weight vector based on cluster membership
3.  run through calc to generate first set of phi, mu, sigma (probably sigma inverse)
4.  iteration - mapper employs phi, mu, sigma calculated in reducer to calc weights for input examples
    and generate partial sums for phi, mu, sigma inverse calc.  

'''

def main():
    #run the first initialization to obtain scale factors
    filePath = '//home//mike-bowles//pyWorkspace//mapReducers//src//mr_glmReg//input.txt'
    mrJob = MrGlmRegInit1(args=[filePath])
    with mrJob.make_runner() as runner:
        runner.run()
    
    
    #run the second initialization to calc lambda starting values
    mrJob = MrGlmRegInit2(args=[filePath])
    with mrJob.make_runner() as runner:
        runner.run()
    
    
    betaPath = "//home//mike-bowles//pyWorkspace//mapReducers//src//mr_glmReg//lambdaBeta.txt"
    fileIn = open(betaPath)
    paramJson = fileIn.read()
    fileIn.close()
    
    
    #Begin iteration to calculate coefficient path
    for i in range(100):
        #run one iteration
        mrJob2 = MrGlmRegIter(args=[filePath])
        with mrJob2.make_runner() as runner:
            runner.run()

if __name__ == '__main__':
    main()