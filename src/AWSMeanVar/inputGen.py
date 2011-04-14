'''
Created on Feb 21, 2011

@author: mike-bowles
'''
from numpy import random

pathname="//home//mike-bowles//pyWorkspace//mapReducers//src//meanVarExamp//"
filename="Input.txt"
fileOut=open(pathname+filename,"w") 

for i in range(100):
    num = random.rand()
    fileOut.write(str(num) + "\n")
    