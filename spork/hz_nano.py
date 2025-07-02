import collections
from collections.abc import Iterator

class Worker:

    def map_only(self, src: str, dst: str):
        with open(dst, 'w') as fp:
            for line in open(src):
                for x in self.map(line):
                    fp.write(str(x) + '\n')


    def map_reduce(self, src: str, dst: str):
        grouped_by_key = collections.defaultdict(list)
        for line in open(src):
            for key, val in self.map(line):
                grouped_by_key[str(key)].append(val)
        with open(dst, 'w') as fp:
            for key, values in grouped_by_key.items():
                for reduced_value in self.reduce(key, values):
                    pair = (key, reduced_value)
                    fp.write(str(pair) + '\n')
        

    def map(self, x):
        """Yield one or more items. 
        """
        assert False, unimplemented

    def reduce(self, key, values: Iterator):
        """Yield one or more items to be associated with the key.
        """
        assert False, 'unimplemented'

