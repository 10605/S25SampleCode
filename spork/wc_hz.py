import re
import hz_worker as hz

class WordCount(hz.Worker):
    
    def map(self, x):
        for word in re.findall('\w+', x.lower()):
            yield (word, 1)

    def reduce(self, word, counts):
        yield sum(counts)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    wc = WordCount()
    wc.do_map_and_shuffle('blue.txt', wc.workers[0])
