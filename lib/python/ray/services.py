from __future__ import print_function

import os
import sys
import time
import subprocess
import string
import random

# Ray modules
import config
import plasma

# all_processes is a list of the scheduler, object store, and worker processes
# that have been started by this services module if Ray is being used in local
# mode.
all_processes = []

def address(host, port):
  return host + ":" + str(port)

def new_port():
  return random.randint(10000, 65535)

def random_name():
  return str(random.randint(0, 99999999))

def cleanup():
  """When running in local mode, shutdown the Ray processes.

  This method is used to shutdown processes that were started with
  services.start_ray_local(). It kills all scheduler, object store, and worker
  processes that were started by this services module. Driver processes are
  started and disconnected by worker.py.
  """
  global all_processes
  successfully_shut_down = True
  # Terminate the processes in reverse order.
  for p in all_processes[::-1]:
    if p.poll() is not None: # process has already terminated
      continue
    p.kill()
    time.sleep(0.05) # is this necessary?
    if p.poll() is not None:
      continue
    p.terminate()
    time.sleep(0.05) # is this necessary?
    if p.poll is not None:
      continue
    successfully_shut_down = False
  if successfully_shut_down:
    print("Successfully shut down Ray.")
  else:
    print("Ray did not shut down properly.")
  all_processes = []

def start_redis(port):
  redis_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../common/thirdparty/redis-3.2.3/src/redis-server")
  p = subprocess.Popen([redis_filepath, "--port", str(port), "--loglevel", "warning"])
  if cleanup:
    all_processes.append(p)

def start_local_scheduler(redis_address, plasma_store_name):
  local_scheduler_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../photon/build/photon_scheduler")
  local_scheduler_name = "/tmp/scheduler{}".format(random_name())
  p = subprocess.Popen([local_scheduler_filepath, "-s", local_scheduler_name, "-r", redis_address, "-p", plasma_store_name])
  if cleanup:
    all_processes.append(p)
  return local_scheduler_name

def start_objstore(node_ip_address, redis_address, cleanup):
  """This method starts an object store process.

  Args:
    node_ip_address (str): The ip address of the node running the object store.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
  """
  plasma_store_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../plasma/build/plasma_store")
  store_name = "/tmp/ray_plasma_store{}".format(random_name())
  p1 = subprocess.Popen([plasma_store_executable, "-s", store_name])

  manager_name = "/tmp/ray_plasma_manager{}".format(random_name())
  p2, manager_port = plasma.start_plasma_manager(store_name, manager_name, redis_address)

  if cleanup:
    all_processes.append(p1)
    all_processes.append(p2)

  return store_name, manager_name, manager_port

def start_worker(address_info, worker_path, cleanup=True):
  """This method starts a worker process.

  Args:
    address_info (dict): This dictionary contains the node_ip_address,
      redis_port, object_store_name, object_store_manager_name, and
      local_scheduler_name.
    worker_path (str): The path of the source code which the worker process will
      run.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits. This is True by default.
  """
  command = ["python",
             worker_path,
             "--node-ip-address=" + address_info["node_ip_address"],
             "--object-store-name=" + address_info["object_store_name"],
             "--object-store-manager-name=" + address_info["object_store_manager_name"],
             "--local-scheduler-name=" + address_info["local_scheduler_name"],
             "--redis-port=" + str(address_info["redis_port"])]
  p = subprocess.Popen(command)
  if cleanup:
    all_processes.append(p)

def start_ray_local(node_ip_address="127.0.0.1", num_workers=0, worker_path=None):
  """Start Ray in local mode.

  Args:
    num_workers (int): The number of workers to start.
    worker_path (str): The path of the source code that will be run by the
      worker.

  Returns:
    This returns a tuple of three things. The first element is a tuple of the
    Redis hostname and port. The second
  """
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "default_worker.py")
  # Start Redis.
  redis_port = new_port()
  redis_address = address(node_ip_address, redis_port)
  start_redis(redis_port)
  time.sleep(0.1)
  # Start Plasma.
  object_store_name, object_store_manager_name, object_store_manager_port = start_objstore(node_ip_address, redis_address, cleanup=True)
  # Start the local scheduler.
  time.sleep(0.1)
  local_scheduler_name = start_local_scheduler(redis_address, object_store_name)
  time.sleep(0.2)
  # Aggregate the address information together.
  address_info = {"node_ip_address": node_ip_address,
                  "redis_port": redis_port,
                  "object_store_name": object_store_name,
                  "object_store_manager_name": object_store_manager_name,
                  "local_scheduler_name": local_scheduler_name}
  # Start the workers.
  for _ in range(num_workers):
    start_worker(address_info, worker_path, cleanup=True)
  time.sleep(0.3)
  # Return the addresses of the relevant processes.
  return address_info
