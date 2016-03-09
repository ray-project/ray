import subprocess32 as subprocess
import os
import atexit
import time

_services_path = os.path.dirname(os.path.abspath(__file__))

all_processes = []

def cleanup():
  global all_processes
  for p, port in all_processes:
    if p.poll() is not None: # process has already terminated
      print "Process at port " + str(port) + " has already terminated."
      continue
    print "Attempting to kill process at port " + str(port) + "."
    p.kill()
    time.sleep(0.05) # is this necessary?
    if p.poll() is not None:
      print "Successfully killed process at port " + str(port) + "."
      continue
    print "Kill attempt failed, attempting to terminate process at port " + str(port) + "."
    p.terminate()
    time.sleep(0.05) # is this necessary?
    if p.poll is not None:
      print "Successfully terminated process at port " + str(port) + "."
      continue
    print "Termination attempt failed, giving up."
  all_processes = []

atexit.register(cleanup)

def start_scheduler(host, port):
  scheduler_address = host + ":" + str(port)
  p = subprocess.Popen([os.path.join(_services_path, "scheduler"), str(scheduler_address)])
  all_processes.append((p, port))

def start_objstore(host, port):
  objstore_address = host + ":" + str(port)
  p = subprocess.Popen([os.path.join(_services_path, "objstore"), str(objstore_address)])
  all_processes.append((p, port))

def start_worker(test_path, host, scheduler_port, worker_port, objstore_port):
  p = subprocess.Popen(["python", test_path, host, str(scheduler_port), str(worker_port), str(objstore_port)])
  all_processes.append((p, worker_port))
