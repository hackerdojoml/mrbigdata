'''
Created on Apr 18, 2011

@author: mike-bowles
'''
from mr_GMixEmInitialize import MrGMixEmInit
from mr_GMixEmIterate import MrGMixEm
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
    #first run the initializer to get starting centroids
    filePath = '//home//mike-bowles//pyWorkspace//mapReducers//src//mr_GMixEm//inputWide.txt'
    mrJob = MrGMixEmInit(args=[filePath])
    with mrJob.make_runner() as runner:
        runner.run()
    
    #pull out the centroid values to compare with values after one iteration
    emPath = "//home//mike-bowles//pyWorkspace//mapReducers//src//mr_GMixEm//intermediateResults.txt"
    fileIn = open(emPath)
    paramJson = fileIn.read()
    fileIn.close()
    
    delta = 10
    #Begin iteration on change in centroids
    while delta > 0.01:
        #parse old centroid values
        oldParam = json.loads(paramJson)
        #run one iteration
        oldMeans = oldParam[1]
        mrJob2 = MrGMixEm(args=[filePath])
        with mrJob2.make_runner() as runner:
            runner.run()
            
        #compare new centroids to old ones
        fileIn = open(emPath)
        paramJson = fileIn.read()
        fileIn.close()
        newParam = json.loads(paramJson)
        
        k_means = len(newParam[1])
        newMeans = newParam[1]
        
        delta = 0.0
        for i in range(k_means):
            delta += dist(newMeans[i],oldMeans[i])
        
        print delta

if __name__ == '__main__':
    main()