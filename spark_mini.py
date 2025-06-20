from __future__ import annotations


from dataclasses import dataclass
import json
import subprocess
from typing import Callable, Iterable

REDUCE_BUFFER_FILE = '_spark_mini-reduce_buffer.tsv'
SORT_BUFFER_FILE = '_spark_mini-sort_buffer.tsv'

class Context:
    sort_on_disk: bool = False

    def textFile(self, fileName: str):
        return TextFile(self, fileName)

    def asRDD(self, xs: list):
        return IteratorRDD(self, xs)

class RDD:
    context: Context

    def _contents(self):
        """Generate each thing in the RDD.
        """
        assert False, 'abstract method called'

    def map(self, fn) -> RDD:
        return Mapper(context=self.context, upstream=self, map_fn=fn)

    def reduceByKey(self, fn) -> RDD:
        if self.context.sort_on_disk:
            return DiskReducer(context=self.context, upstream=self, reduce_fn=fn)
        else:
            return Reducer(context=self.context, upstream=self, reduce_fn=fn)

    def flatMap(self, fn) -> RDD:
        return FlatMapper(context=self.context, upstream=self, map_fn=fn)

    def collect(self) -> list:
        return list(self._contents())
    
    def take(self, n) -> list:
        buffer = []
        for i, x in enumerate(self._contents()):
            if i > n: break
            buffer.append(x)
        return buffer

@dataclass
class TextFile(RDD):
    context: Context
    fileName: str

    def _contents(self):
        for line in open(self.fileName):
            yield line
    
@dataclass
class IteratorRDD(RDD):
    context: Context
    contents: Iterable

    def _contents(self):
        for x in self.contents:
            yield x

@dataclass
class Mapper(RDD):
    context: Context
    upstream: RDD
    map_fn: Callable

    def _contents(self):
        for x in self.upstream._contents():
            yield self.map_fn(x)

@dataclass
class FlatMapper(RDD):
    context: Context
    upstream: RDD
    map_fn: Callable

    def _contents(self):
        for x in self.upstream._contents():
            for y in x:
                yield self.map_fn(y)

@dataclass
class Reducer(RDD):
    context: Context
    upstream: RDD
    reduce_fn: Callable

    def _sorted_upstream_contents(self):
        """Return the contents of the upstream RDD, sorted by key.

        Upstream RDD must produce pairs.
        """
        pairs = list(self.upstream._contents())
        pairs.sort(key=lambda pair:pair[0])
        return pairs

    def _contents(self):
        # complicated but fairly general
        buffer = {}
        for key, value in self._sorted_upstream_contents():
            if key in buffer:
                # reduce new value with the existing aggregation
                buffer[key] = self.reduce_fn(buffer[key], value)
            else:
                # flush buffer
                for stored_kv in buffer.items():
                    yield stored_kv
                buffer.clear()
                buffer[key] = value
        # flush buffer
        for stored_kv in buffer.items():
            yield stored_kv

@dataclass
class DiskReducer(Reducer):

    def _sorted_upstream_contents(self):
        """Return the contents of the upstream RDD, sorted by key.

        Upstream RDD must product pairs.

        If REDUCE_BUFFER_FILE and SORT_BUFFER_FILE are given,
        then this is done in a memory-efficient way with
        a unix sort.  In this case keys and values are
        serialized as json, and each line is a tab-separated
        key, value pair terminated with a linefeed.
        """
        # convert to tab-sep json format
        with open(REDUCE_BUFFER_FILE, 'w') as fp:
            for (key, value) in self.upstream._contents():
                fp.write(json.dumps(key) + '\t'
                         + json.dumps(value) + '\n')
        # sort
        sort_cmd = 'LC_ALL=C sort'
        sort_inp = REDUCE_BUFFER_FILE
        sort_outp = SORT_BUFFER_FILE
        command = f'{sort_cmd} -k1,2 < {sort_inp} > {sort_outp}'
        subprocess.check_call(command,shell=True)
        # read in sorted lines and convert back to python
        for line in open(SORT_BUFFER_FILE):
            str_key, str_value = line.rstrip().split('\t')
            yield json.loads(str_key), json.loads(str_value)
