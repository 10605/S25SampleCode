import nano

sc = nano.Context()
sc.sort_on_disk = True

wc = sc.textFile('../data/redcorpus.txt') \
       .flatMap(lambda line:line.lower().split(' ')) \
       .map(lambda word: (word, 1)) \
       .reduceByKey(lambda a,b: a+b)

output = wc.take(10)

# debugging
lines = sc.textFile('data/redcorpus.txt')
wordlists = lines.map(lambda line:line.lower().split(' '))
wordpairs = wordlists.flatMap(lambda word: (word, 1))

