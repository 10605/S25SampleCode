import spark_mini

sc = spark_mini.Context()
sc.sort_on_disk = True

wc = sc.textFile('data/redcorpus.txt') \
       .map(lambda line:line.lower().split(' ')) \
       .flatMap(lambda word: (word, 1)) \
       .reduceByKey(lambda a,b: a+b)

print(wc.take(10))

lines = sc.textFile('data/redcorpus.txt')
wordlists = lines.map(lambda line:line.lower().split(' '))
wordpairs = wordlists.flatMap(lambda word: (word, 1))

