#Anson Rosenthal
#6/18/2013
#General Pipeline implementation in Python
# v1.0
#########################################
#   Provides abstraction for chaining sequential operations on data using multithreading
#   and synchronous queues for higher execution throughput
#
#   Pipeline is fed by a generator, data then passes through any number of functions, each of which are linked by 
#       synchronous queues and executed on separate threads.
#   
#   Considered splatting data passed from queues to function at next stage...decided not to for now
#
#   synchronized Queue is very slow...what to do about it.
#
from queue import Queue
from collections import deque
from threading import Thread

#attempt to get some speed from a nonblocking queue structure - crashes
class asyQueue(deque):
    def get(self):
        if self.empty():
            return self.get()
        else:
            return self.pop()
    
    def put(self,x):
        self.append(x)
    
    def empty(self):
        return len(self) == 0

class pipelineWorker(Thread):
	
    def __init__(self, fun, inQ=None, source=False):
        super(pipelineWorker, self).__init__()
        self.inQ = inQ
        self.outQ = None
        self.fun = fun
        self.prev = None
        self.next_worker = None
        self.source = source
        self.done = False

    def isDone(self):
        if self.source: 
            return self.done
        else:
            return self.done and self.inQ.empty() and self.prev.isDone()
        
    def run(self):
        if(self.next_worker):
            self.next_worker.start()
        if self.source:
            for task in self.inQ:
                if self.outQ:
                    self.outQ.put(self.fun(task))
                else:
                    self.fun(task)
            self.done = True
        else:
            while(not self.prev.isDone() or not self.inQ.empty()):
                task = self.inQ.get()
                if self.outQ:
                    self.outQ.put(self.fun(task))
                else:
                    self.fun(task)
            self.done = True
            
    def chain(self, nextworker):
        self.next_worker = nextworker
        self.outQ = Queue()
        nextworker.inQ = self.outQ
        nextworker.prev = self
	
class Pipeline():
	
    def __init__(self, datagen, *funs):
        self.datagen = datagen
        self.funs = funs
        self.head = None
        self.tail = None
        
    def make_workers(self):
        self.head = pipelineWorker(self.funs[0], self.datagen, True)
        self.tail = self.head
        for i in range(1,len(self.funs)):
            self.tail.chain(pipelineWorker(self.funs[i]))
            self.tail = self.tail.next_worker			
 
    def run(self):
        self.make_workers()
        self.head.start()
        
    def isDone(self):
        return self.tail.isDone()  