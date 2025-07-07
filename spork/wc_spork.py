import re

import spork_micro as spork

sc = spork.Context()

wc = sc.textFile('../data/bluecorpus.txt') \
       .map(lambda line: line.lower()) \
       .flatMap(lambda line: re.findall(r'\w+', line)) \
       .map(lambda word: (word, 1)) \
       .reduceByKey(lambda a,b: a+b)

freqs = dict(wc.collect())
print('freq of "this":', freqs['this'])


