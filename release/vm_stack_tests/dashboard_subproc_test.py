#!/usr/bin/env python

import subprocess
import signal
import time
import sys

args_nosh = ['ray', 'dashboard', 'cluster_launcher_config_aws.yaml']
args = ['sh', '-c', '"ray dashboard cluster_launcher_config_aws.yaml"']
cat_args = ['cat', 'binary']

#proc = subprocess.Popen(cat_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
proc = subprocess.Popen(args_nosh, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#proc = subprocess.Popen(' '.join(args), shell=True)
#proc = subprocess.Popen(' '.join(args), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if len(sys.argv) != 2:
    raise ValueError(f'Expected two args, got {sys.argv}')

codepath = int(sys.argv[1])
print(f'codepath {codepath}')

if codepath == 0:
    # This reproduces the issue
    print('sleeping 5')
    time.sleep(5)
if codepath == 1:
    # This reproduces the issue
    print('sleeping 5')
    time.sleep(5)
    subprocess.Popen(['sleep', '1']).wait()
elif codepath == 2:
    # This reproduces the issue
    print('sleeping 5 (subproc)')
    subprocess.Popen(['sleep', '5']).wait()
elif codepath == 3:
    # This does not reproduce the issue.
    print('enter any key (subproc)')
    subprocess.Popen(['read', '-n', '1']).wait()
if codepath == 4:
    # This reproduces the issue.
    print('sleeping 5 (subshell subproc)')
    subprocess.Popen(' '.join(['/bin/sh', '-c', '"sleep 5"']), shell=True).wait()
if codepath == 5:
    # This hangs -- stdin does not go
    input('enter any key (not subproc)')
elif codepath == 6:
    # Reproduces
    print('waiting with random read process')
    a = subprocess.Popen(['read', '-n', '1'])
    time.sleep(5)
    a.send_signal(signal.SIGTERM)
    a.wait()
elif codepath == 7:
    # This reproduces the issue
    print('sleeping 1')
    time.sleep(1)

print('killing')
proc.send_signal(signal.SIGTERM)
proc.wait()


'''
sleep 5 reproduces the issue when starting ray dashboard via popen
    shell doesnt matter
    pipe/devnull doesnt matter

when switching to `enter any key`, e.g. read via popen, the problem goes away

Experiments:
    * ray dashboard + sleep 5 = issue
    * ray dashboard + read.wait = no issue
    * 

'''
