"""Very simple map-reduce system.
"""

import ast
import collections
import fire
import json
import os
from pprint import pprint
from subprocess import Popen, PIPE
from subprocess import run as run_subproc
import shlex
import sys
import time
from tqdm import tqdm

#TODO replace with just workers.txt file?
DEFAULT_CONFIG_FILE = 'cloud_config.json'


class CloudBase:
    """Base class for working with an ec2 cloud.
    """
    def __init__(self, config_file=DEFAULT_CONFIG_FILE):
        """Load state previously specified with 'config' from disk.
        """
        try:
            with open(config_file) as fp:
                config = json.load(fp)
            for key, value in config.items():
                setattr(self, key, value)
        except FileNotFoundError:
            print(f'warning: no config at {config_file}')
            self.cloud_username = 'ec2-user'
            self.keypair_file = 'hazsoup.pem'
            self.workers = None

    #TODO "def ssh_cmd(worker, command)"

    def _defined_attr(self):
        """Externally visible attributes of this object.
        """
        return [a for a in self.__dict__ if not a.startswith('_')]

    def _save(self, config_file=DEFAULT_CONFIG_FILE):
        """Save the defined attribute values to the config file.
        """
        with open(config_file, 'w') as fp:
            config = {a:getattr(self, a) for a in self._defined_attr()}
            json.dump(config, fp)
        print(f'saved to {config_file}: {self._defined_attr()}')

    def setattr(self, attr, value, split=True, config_file=DEFAULT_CONFIG_FILE):
        """Set an attribute, like workers, keypair_file, or cloud_username.

        Modifies the stored config file.
        """
        if split:
            value = value.split()
        setattr(self, attr, value)
        self._save()

    def _completion_progress(self, processes, delay=0.25):
        """A progress bar for process completion.
        """
        for n, _ in tqdm(enumerate(processes), 'processes running'):
            finished = []
            while len(finished) < n:
                finished = [p for p in processes if p.poll() is not None]
                time.sleep(delay)

    def _report(self, proc, worker):
        """Echo outputs of processes from a worker.
        """
        def report_stdx(what, proc_stdx, worker):
            # report proc.stdout or proc.stderr
            outp = proc_stdx if isinstance(proc_stdx, str) else proc_stdx.read()
            if outp:
                print(f'{what} {worker}'.center(60, '='))
                print(outp, end='')
        if proc.returncode:
            print(f'returncode {worker}: {proc.returncode}')
        report_stdx('stdout', proc.stdout, worker) 
        report_stdx('stderr', proc.stderr, worker)

    def ssh(self, command):
        """Run a shell command on all workers.
        """
        for worker in self.workers:
            sh_tokens = (['ssh', '-i', self.keypair_file]
                         + [f'{self.cloud_username}@{worker}']
                         + shlex.split(command))
            proc = run_subproc(sh_tokens, capture_output=True, text=True)
            self._report(proc, worker)
                
    def sshp(self, command):
        """Run a shell command on all workers in parallel.
        """
        processes = [
            Popen(['ssh', '-i', self.keypair_file,
                   f'{self.cloud_username}@{worker}']
                  + shlex.split(command),
                  stderr=PIPE, stdout=PIPE, text=True)
            for worker in self.workers]
        self._completion_progress(processes)
        for proc, worker in zip(processes, self.workers):
            self._report(proc, worker)
            proc.wait()

    def ssh1(self, command):
        """Run a shell command on one worker.
        """
        worker = self.workers[0]
        proc = run_subproc(
            ['ssh', '-i', self.keypair_file,
             f'{self.cloud_username}@{worker}']
            + shlex.split(command),
            capture_output=True, text=True)
        self._report(proc, worker)

    def setup_ec2(self):
        """Commands needed to initialize an ec2 cluster.
        """
        print('installing pip')
        self.sshp("sudo yum install python3-pip -y")
        print('installing fire')
        self.sshp("pip3 install fire")
        print('uploading core')
        self.upload(
            f"hz_worker.py reduce_util.py {DEFAULT_CONFIG_FILE} {self.keypair_file}")
        self.sshp(f"chmod 400 {self.keypair_file}")

class FileSystem(CloudBase):

    def upload(self, filenames):
        """Copy a file to all workers.
        """
        for worker in tqdm(self.workers):
            sh_tokens = (
                ['scp', '-i', self.keypair_file]
                + shlex.split(filenames)
                + [f'{self.cloud_username}@{worker}:'])
            proc = run_subproc(sh_tokens, capture_output=True, text=True)
            self._report(proc, worker)

    def put(self, src, dst):
        """Shard a local file and upload shards to the workers.
        """
        line_ctr = collections.Counter()
        # create a process for each worker that can accept text lines
        # via its stdin
        def sh_tokens(worker):
            return (['ssh', '-i', self.keypair_file]
                    + [f'{self.cloud_username}@{worker}']
                    + shlex.split(f'cat > {dst}'))
        worker_processes = [
            Popen(sh_tokens(worker), text=True, stdin=PIPE)
            for worker in self.workers
        ]
        # upload the local file
        for line in tqdm(open(src)):
            # send line to the worker with this index
            worker_idx = hash(line) % len(self.workers)
            worker_processes[worker_idx].stdin.write(line)
            # record some statistics
            line_ctr[src] += 1
            line_ctr[f'{self.workers[worker_idx]}:{dst}'] += 1
        # close the worker processes and wait for them to finish
        for proc in tqdm(worker_processes):
            proc.stdin.close()
            proc.wait()
        # echo statistics
        pprint(line_ctr)
        
    def get_merge(self, src, dst):
        """Get remote shards and collect them into a local file.
        """
        line_ctr = collections.Counter()
        def sh_tokens(worker):
            return (['ssh', '-i', self.keypair_file]
                    + [f'{self.cloud_username}@{worker}']
                    + shlex.split(f'cat < {src}'))
        with open(dst, 'w') as fp:
            for worker in tqdm(self.workers):
                # download the data from that worker to local file
                proc = Popen(
                    sh_tokens(worker), text=True, stdout=PIPE)
                for line in proc.stdout:
                    fp.write(line)
                    # record some statistics
                    line_ctr[f'{worker}:{src}'] += 1
                    line_ctr[dst] += 1
                # wait for process to end
                proc.wait()
        # echo statistics
        pprint(line_ctr)

    def head(self, src):
        """Print the head of the output of the first worker.
        """
        worker = self.workers[0]
        proc = run_subproc(
            ['ssh', '-i', self.keypair_file, 
             f'{self.cloud_username}@{worker}',
             'head', src],
            capture_output=True, text=True)
        print(proc.stdout, end='')

    def tail(self, src):
        """Print the tail of the output of the last worker.
        """
        worker = self.workers[-1]
        proc = run_subproc(
            ['ssh', '-i', self.keypair_file, 
             f'{self.cloud_username}@{worker}',
             'tail', src],
            capture_output=True, text=True)
        print(proc.stdout, end='')

class Driver(CloudBase):
    """Invokes the workers appropriately to complete a task.
    """


    def map_only(self, main_script, main_class, src, dst):
        """Run a map-only job.
        """
        self.sshp(f'python3 -m fire {main_script} {main_class}'
                  + f' do_map --src {src} --dst {dst}')

    def map_reduce(self, main_script, main_class, src, dst):

        print('map and shuffle phase')
        # shuffle phase - cannot use sshp since each command mentions
        # is a different this_worker
        def shuffle_command(worker):
            return ['ssh', '-i', self.keypair_file,
                    f'{self.cloud_username}@{worker}',
                    'python3', '-m', 'fire', main_script, main_class, 
                   'do_map_and_shuffle', 
                    '--src', src,
                    '--config_file', DEFAULT_CONFIG_FILE,
                    '--this_worker', worker]
        processes = [
            Popen(shuffle_command(worker), text=True, stderr=PIPE, stdout=PIPE)
            for worker in self.workers]
        self._completion_progress(processes)
        for proc, worker in tqdm(zip(processes, self.workers)):
            self._report(proc, worker)
            proc.wait()

        print('gather and reduce phase')
        # rather and reduce phase
        self.sshp(f'python3 -m fire {main_script} {main_class}'
                  + f' do_gather_reduce --src {src} --dst {dst}'
                  + f' --config_file {DEFAULT_CONFIG_FILE}')
        

    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        fire.Fire(dict(
            fs = FileSystem,
            run = Driver,
            cloud = CloudBase))
