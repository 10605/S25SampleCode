import hazsoup as hs

class MyWorker(hs.Worker):
    
    def map(self, x):
        for word in x.lower().split():
            yield (word, 1)
