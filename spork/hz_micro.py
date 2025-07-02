import collections
from collections.abc import Iterator
import subprocess
import json

import reduce_util

REDUCE_BUFFER_FILE = '/tmp/micro-reduce_buffer.tsv'
SORT_BUFFER_FILE = '/tmp/micro-sort_buffer.tsv'

class Worker:

    def map_only(self, src: str, dst: str):
        with open(dst, 'w') as fp:
            for line in open(src):
                for x in self.map(line):
                    fp.write(str(x) + '\n')


    def map_reduce(self, src: str, dst: str):
        # save the map output as a text file
        with open(REDUCE_BUFFER_FILE, 'w') as fp:
            for line in open(src):
                for key, value in self.map(line):
                    fp.write(json.dumps(key) + '\t'
                             + json.dumps(value) + '\n')
        # sort the map output
        sort_cmd = 'LC_ALL=C sort'
        sort_inp = REDUCE_BUFFER_FILE
        sort_outp = SORT_BUFFER_FILE
        command = f'{sort_cmd} -k1,2 < {sort_inp} > {sort_outp}'
        subprocess.check_call(command, shell=True)

        # create a generator for the pairs
        def pair_generator():
            for line in open(SORT_BUFFER_FILE):
                str_key, str_value = line.rstrip().split('\t')
                yield json.loads(str_key), json.loads(str_value)
                
        # convert to the format for calling reduce
        with open(dst, 'w') as fp:
            for key, values in reduce_util.ReduceReady(pair_generator()):
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

