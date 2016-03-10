import subprocess32 as subprocess
import os
import atexit
import time

_services_path = os.path.dirname(os.path.abspath(__file__))

all_processes = []

def cleanup():
  global all_processes
  for p, address in all_processes:
    if p.poll() is not None: # process has already terminated
      print "Process at address " + address + " has already terminated."
      continue
    print "Attempting to kill process at address " + address + "."
    p.kill()
    time.sleep(0.05) # is this necessary?
    if p.poll() is not None:
      print "Successfully killed process at address " + address + "."
      continue
    print "Kill attempt failed, attempting to terminate process at address " + address + "."
    p.terminate()
    time.sleep(0.05) # is this necessary?
    if p.poll is not None:
      print "Successfully terminated process at address " + address + "."
      continue
    print "Termination attempt failed, giving up."
  all_processes = []

atexit.register(cleanup)

def start_scheduler(scheduler_address):
  p = subprocess.Popen([os.path.join(_services_path, "scheduler"), scheduler_address])
  all_processes.append((p, scheduler_address))

def start_objstore(objstore_address):
  p = subprocess.Popen([os.path.join(_services_path, "objstore"), objstore_address])
  all_processes.append((p, objstore_address))

def start_worker(test_path, scheduler_address, objstore_address, worker_address):
  p = subprocess.Popen(["python",
                        test_path,
                        "--scheduler-address=" + scheduler_address,
                        "--objstore-address=" + objstore_address,
                        "--worker-address=" + worker_address])
  all_processes.append((p, worker_address))
