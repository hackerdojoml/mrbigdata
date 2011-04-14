'''
Created on Feb 21, 2011

@author: mike-bowles
'''
from numpy import random
import json

pathname="//home//mike-bowles//pyWorkspace//mapReducers//src//mrjMeanVar//"
filename="inputJson.txt"
fileOut=open(pathname+filename,"w") 

arrayOut = []
for i in range(100):
    num = random.rand()
    arrayOut.append(num)


fileOut.write(json.dumps(arrayOut))
fileOut.close()
    