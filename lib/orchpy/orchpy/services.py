import subprocess32 as subprocess
import os
import atexit
import time

_services_path = os.path.dirname(os.path.abspath(__file__))

all_processes = []

def cleanup():
  global all_processes
  timeout_sec = 5
  for p in all_processes:
    p_sec = 0
    for second in range(timeout_sec):
      if p.poll() == None:
        time.sleep(0.1)
        p_sec += 1
        if p_sec >= timeout_sec:
          p.kill() # supported from python 2.6
          print 'helper processes shut down!'
  all_processes = []

atexit.register(cleanup)

def start_scheduler(scheduler_address):
  p = subprocess.Popen([os.path.join(_services_path, "scheduler_server"), str(scheduler_address)])
  all_processes.append(p)

def start_objstore(objstore_address):
  p = subprocess.Popen([os.path.join(_services_path, "objstore"), str(objstore_address)])
  all_processes.append(p)
