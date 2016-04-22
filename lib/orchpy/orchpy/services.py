import subprocess32 as subprocess
import os
import atexit
import time

import orchpy
import orchpy.worker as worker

_services_path = os.path.dirname(os.path.abspath(__file__))

all_processes = []
driver = None

IP_ADDRESS = "127.0.0.1"
TIMEOUT_SECONDS = 5

def address(host, port):
  return host + ":" + str(port)

scheduler_port_counter = 0
def new_scheduler_port():
  global scheduler_port_counter
  scheduler_port_counter += 1
  return 10000 + scheduler_port_counter

worker_port_counter = 0
def new_worker_port():
  global worker_port_counter
  worker_port_counter += 1
  return 40000 + worker_port_counter

objstore_port_counter = 0
def new_objstore_port():
  global objstore_port_counter
  objstore_port_counter += 1
  return 20000 + objstore_port_counter

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

  global driver
  if driver is not None:
    orchpy.disconnect(driver)
  else:
    orchpy.disconnect()
  driver = None

# atexit.register(cleanup)

def start_scheduler(scheduler_address):
  p = subprocess.Popen([os.path.join(_services_path, "scheduler"), scheduler_address])
  all_processes.append((p, scheduler_address))

def start_objstore(scheduler_address, objstore_address):
  p = subprocess.Popen([os.path.join(_services_path, "objstore"), scheduler_address, objstore_address])
  all_processes.append((p, objstore_address))

def start_worker(test_path, scheduler_address, objstore_address, worker_address):
  p = subprocess.Popen(["python",
                        test_path,
                        "--scheduler-address=" + scheduler_address,
                        "--objstore-address=" + objstore_address,
                        "--worker-address=" + worker_address])
  all_processes.append((p, worker_address))

def start_cluster(driver_worker=None, num_workers=0, worker_path=None):
  global driver
  if num_workers > 0 and worker_path is None:
    raise Exception("Attempting to start a cluster with some workers, but `worker_path` is None.")
  scheduler_address = address(IP_ADDRESS, new_scheduler_port())
  objstore_address = address(IP_ADDRESS, new_objstore_port())
  start_scheduler(scheduler_address)
  time.sleep(0.1)
  start_objstore(scheduler_address, objstore_address)
  time.sleep(0.2)
  if driver_worker is not None:
    orchpy.connect(scheduler_address, objstore_address, address(IP_ADDRESS, new_worker_port()), driver_worker)
    driver = driver_worker
  else:
    orchpy.connect(scheduler_address, objstore_address, address(IP_ADDRESS, new_worker_port()))
  for _ in range(num_workers):
    start_worker(worker_path, scheduler_address, objstore_address, address(IP_ADDRESS, new_worker_port()))
  time.sleep(0.5)
