from subprocess import Popen, PIPE
import shlex
import sys
import time
from tqdm import tqdm

def progress_bar_trial():
    processes = [
        Popen(
            f'sleep {i}; cat ../data/bluecorpus.txt; echo "process {i} done"',
            stdout=PIPE, text=True, shell=True)
        for i in range(1, 11)]

    print('sample command:', processes[0].args)

    def split(proc):
        finished = {True:[], False:[]}
        for p in proc:
            finished[p.poll() is not None].append(p)
        return finished

    for n in tqdm(range(len(processes)), position=1, desc='finished'):
        finished = split(processes)
        while len(finished[True]) < n:
            finished = split(processes)
            if finished[False]:
                sample_proc = finished[False][0]
                for line in tqdm(
                        sample_proc.stdout.readlines(),
                        position=0,
                        desc=f'{sample_proc.args} lines'):
                    time.sleep(0.001)
            time.sleep(0.5)
        
        
def _report(proc, worker, stderr=False, stdout=False):
    def report_stdx(what, proc_stdx, worker):
        """report stderr or stdout."""
        if proc_stdx is None:
            outp = None
        elif isinstance(proc_stdx, str):
            outp = proc_stdx
        else:
            outp = proc_stdx.read()
        if outp:
            print(f'{what} {worker}'.center(60, '='))
            print(outp, end='')
    if proc.returncode:
        print(f'returncode {worker}: {proc.returncode}')
    report_stdx('stdout', proc.stdout, worker) 
    report_stdx('stderr', proc.stderr, worker)

def ssh_io_trial(filename):
    # (1) stderr is just printed to stderr w/o stderr=PIPE 
    # (2) same for stdout
    # (3) _report can be before or after wait
    proc = Popen(
        shlex.split(f'ssh sijo.ml.cmu.edu "sleep 5; cat {filename}"'),
        stderr=PIPE, stdout=PIPE,
        text=True)
    _report(proc, 'sijo.ml.cmu.edu')
    proc.wait()
    print(f'process: <{" ".join(proc.args)}>')
    print('proc', proc)

progress_bar_trial()
#ssh_io_trial(sys.argv[1])
