#!/usr/bin/env python

import subprocess
import signal
import time

#args = ['ray', 'dashboard', 'cluster_launcher_config_aws.yaml']
args = ['sh', '-c', '"ray dashboard cluster_launcher_config_aws.yaml"']

proc = subprocess.Popen(' '.join(args), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print('enter any key')
subprocess.Popen(['read', '-n', '1']).wait()
print('killing')
proc.send_signal(signal.SIGTERM)
proc.wait()
