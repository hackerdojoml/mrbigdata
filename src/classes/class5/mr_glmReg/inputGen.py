'''
Created on May 4, 2011

@author: mike-bowles
'''

from numpy import random
import json

pathname="//home//mike-bowles//pyWorkspace//mapReducers//src//mr_glmReg//"
filename="input.txt"
fileOut=open(pathname+filename,"w") 
#generate a simple example.  
# y = x1 + x2
# o1 = x1 + e1 + e2
# o2 = x2 + e1 + e3
# output = [y,[o1,o2]]


for i in range(1000):
    U = random.uniform(low=0.0, high=10, size=2)
    N = random.normal(loc=0.0, scale=1.0, size=3)
    o1 = U[0] + N[0] + N[1]
    o2 = U[1] + N[0] + N[2]
    y = U[0] + U[1]
    
    outString = json.dumps([y,[o1,o2]]) + "\n"
    fileOut.write(outString)
        
fileOut.close()

