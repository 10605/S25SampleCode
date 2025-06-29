"""Very simple map-reduce system.
"""

import ast
import collections
import fire
import json
import os
from subprocess import Popen, PIPE
from subprocess import run as run_subproc
from subprocess import run as run_subproc
import time
from tqdm import tqdm

class FileSystem:
    """Lightweight distributed file system.
    """
    cwd: str
    workers: list[str]

    def __init__(self, config_file='mr-config.json'):
        """Load state previously specified with 'config' from disk.
        """
        try:
            with open(config_file) as fp:
                config = json.load(fp)
                print(f'{config=}')
            self.cwd = config['cwd']
            self.workers = config['workers']
        except FileNotFoundError:
            print(f'warning: no config at {config_file}')

    def config(self, cwd, workers, config_file='mr-config.json'):
        """Configure working directory and list of remote workers for
        subsequent command-line calls.
        """
        with open(config_file, 'w') as fp:
            config = dict(cwd=cwd, workers=workers.split(","))
            json.dump(config, fp)
            print(f'configuration at {config_file}')

    # utils

    def _worker_file(self, filename):
        """Location of file for a remote worker."""
        return f'{self.cwd}/{filename}'

    # file system commands

    def ls(self, opts='lh', pattern='*'):
        """List files matching the pattern on each remote worker.

        Opts are options for 'ls', as a single string with no leading
        dash.
        """
        for worker in self.workers:
            proc = run_subproc(
                f'ssh {worker} ls -{opts} {self._worker_file(pattern)}',
                capture_output=True, text=True, shell=True)
            print(worker.center(60, '-'))
            print(proc.stdout, end='')
            if proc.stderr:
                print(' !! error !! '.center(60, '-'))
                print(proc.stderr, end='')

    def put(self, src, dst):
        """Shard a local file and upload shards to the workers.
        """
        line_ctr = collections.Counter()
        # create a process for each worker that can accept text lines
        # via its stdin
        worker_processes = [
            Popen(f'ssh {worker} "cat > {self._worker_file(dst)}"',
                  text=True, shell=True, stdin=PIPE)
            for worker in self.workers
        ]
        # upload the local file
        for line in open(src):
            # send line to the worker with this index
            worker_idx = hash(line) % len(self.workers)
            worker_processes[worker_idx].stdin.write(line)
            # record some statistics
            line_ctr[src] += 1
            line_ctr[f'{self.workers[worker_idx]}:{dst}'] += 1
        # close the worker processes and wait for them to finish
        for proc in worker_processes:
            proc.stdin.close()
            proc.wait()
        # echo statistics
        print(line_ctr)
        
    def get_merge(self, src, dst):
        """Get remote shards and collect them into a local file.
        """
        line_ctr = collections.Counter()
        with open(dst, 'w') as fp:
            for worker in self.workers:
                # download the data from that worker to local file
                proc = Popen(
                    f'ssh {worker} "cat < {self._worker_file(src)}"',
                    text=True, shell=True, stdout=PIPE)
                for line in proc.stdout:
                    fp.write(line)
                    # record some statistics
                    line_ctr[f'{worker}:{src}'] += 1
                    line_ctr[dst] += 1
                # wait for process to end
                proc.wait()
        # echo statistics
        print(line_ctr)

    def head(self, src):
        worker = self.workers[0]
        proc = run_subproc(
            f'ssh {worker} "head {self._worker_file(src)}"',
            capture_output=True, text=True, shell=True)
        print(proc.stdout, end='')

    def tail(self, src):
        worker = self.workers[-1]
        proc = run_subproc(
            f'ssh {worker} "tail {self._worker_file(src)}"',
            capture_output=True, text=True, shell=True)
        print(proc.stdout, end='')

    # TODO: upload files to workers
    def init_workers(self, *files):
        """
        """
        # make sure cwd exists on each worker, and upload
        # hazsoup.py and other files there
        ...
        
class Worker:
    """An abstract worker for map-reduce tasks.
    """

    def _worker_file(self, cwd, filename):
        """Location of file for a remote worker."""
        return f'{cwd}/{filename}'

    # utility routines for mappers and reducers

    def as_str(self, obj):
        """Convert a Python object to a string.
        """
        return str(obj)

    def from_str(self, serialized_obj: str):
        """Convert a string back to a Python object.
        """
        return ast.literal_eval(serialized_obj)

    # abstract routines to be implemented by instances via worker.map
    # and then executed by commands like

    # py -m fire hs_wc.py MyWorker do_map DIR SRC DST

    def do_map(self, cwd, src, dst):
        """
        """
        with open(self._worker_file(cwd, dst), 'w') as fp:
            for line in open(self._worker_file(cwd, src)):
                for x in self.map(line):
                    fp.write(self.as_str(x) + '\n')

    def map(self, x):
        """Yield one or more items. 

        Map will be called repeatedly and outputs will be sent to the
        reducer in a map-reduce, or else serialized and written out to
        the destination file for a map-only job.
        """
        assert False, unimplemented

    # TODO
    def scatter_keyed_shards(self, cwd, src_worker_name, src, dst, workers):
        """Shard src by key, and send shards to workers for sorting by key.

        Names of shards are {dst}-from-{src_worker_name} so they
        can be collected.
        """
        ...
        
    # TODO
    def do_gather_reduce(self, src, dst, workers, wd):
        """Merge shards generated by scatter_keyed_shards and reduce.

        Specifically, merge labeled {src}-from-{worker}, and stream
        the output of that sort process to the self.reduce(key,
        values).
        """
        ...

    def reduce(self, accum, value2, initial_accum=lambda x:x):
        """
        """
        assert False, 'unimplemented'


# TODO
class Driver:
    """Invokes the workers appropriately to complete a task.
    """

    def map_only(src, dst, *scripts):
        ...

    def map_reduce(src, dst, *scripts):
        ...
    
if __name__ == "__main__":
    #fire.Fire(dict(fs = FileSystem))
    test = ((key, ch) for key in 'william w cohen'.split() for ch in key)
    rrd = ReduceReadyData(test)
