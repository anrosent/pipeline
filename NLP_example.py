#Anson Rosenthal
#6/18/2013
#Illustration of General Pipeline implementation in Python
#########################################
#Simple application of pipeline flow of computation in Natural Language Processing#

import os
from sys import argv
from pipeline import Pipeline
from collections import Counter

#I: directory name
#O: generator of files in directory
def getFileNames(directory):
    return (directory + "\\" + name for name in os.listdir(directory))

#I: filepath
#O: generator of lines in document    
def getFileContents(name):
    return open(name)
    
#I: filename
#O: Counter of word frequency counts    
def frequencyCount(filename):
    c = Counter()
    with open(filename) as f:
        for line in f:
            for word in line.split():
                c[word] += 1
        return c

#I: mapping from words to frequency in a document
#O: mapping with top half of most common words pruned out
def pruneCommon(mapping):
    mean = sum(mapping.values())/len(mapping)
    return {k:v for k,v in mapping.items() if v < mean}

#I: mapping of least common words in a document to their frequencies    
#Side effects: write out all words with frequencies to one file
out = None
def writer(mapping):
    for k,v in mapping.items():
        out.write("%s: %s\n"%(k,v))
    
def main_pipeline(filedir):
    #point it somewhere with a bunch of text files
    pipe = Pipeline(getFileNames(filedir), frequencyCount, pruneCommon, writer)
    pipe.run()
    while(not pipe.isDone()):
        pass
    out.close()

def main_nopipeline(filedir):
    for file in getFileNames(filedir):
        writer(pruneCommon(frequencyCount(file)))
        
    
if __name__ == '__main__':
    dir = argv[1]
    
    if len(argv) < 3:
        print("Usage: python NLP_example.py [file directory] [pipeline | nopipeline]")
    else:
        out = open('outfile_%s.txt'%argv[2],'w')
        if argv[2] == 'nopipeline':    
            main_nopipeline(dir)
        elif argv[2] == 'pipeline':
            main_pipeline(dir)
        else:
            print("Usage: python NLP_example.py [file directory] [-pipeline | -nopipeline]")