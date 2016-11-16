from __future__ import print_function

import psutil
import os
import random
import signal
import sys
import subprocess
import string
import time

# Ray modules
import config
import plasma

# all_processes is a list of the scheduler, object store, and worker processes
# that have been started by this services module if Ray is being used in local
# mode.
all_processes = []

# True if processes are run in the valgrind profiler.
RUN_PHOTON_PROFILER = False
RUN_PLASMA_MANAGER_PROFILER = False
RUN_PLASMA_STORE_PROFILER = False

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
    if RUN_PHOTON_PROFILER or RUN_PLASMA_MANAGER_PROFILER or RUN_PLASMA_STORE_PROFILER:
      os.kill(p.pid, signal.SIGINT) # Give process signal to write profiler data.
      time.sleep(0.1) # Wait for profiling data to be written.
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

def start_redis(num_retries=20, cleanup=True):
  """Start a Redis server.

  Args:
    num_retries (int): The number of times to attempt to start Redis.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.

  Returns:
    The port used by Redis.

  Raises:
    Exception: An exception is raised if Redis could not be started.
  """
  redis_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../common/thirdparty/redis-3.2.3/src/redis-server")
  counter = 0
  while counter < num_retries:
    if counter > 0:
      print("Redis failed to start, retrying now.")
    port = new_port()
    p = subprocess.Popen([redis_filepath, "--port", str(port), "--loglevel", "warning"])
    time.sleep(0.1)
    # Check if Redis successfully started (or at least if it the executable did
    # not exit within 0.1 seconds).
    if p.poll() is None:
      if cleanup:
        all_processes.append(p)
      return port
    counter += 1
  raise Exception("Couldn't start Redis.")

def start_local_scheduler(redis_address, plasma_store_name, cleanup=True):
  local_scheduler_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../photon/build/photon_scheduler")
  if RUN_PHOTON_PROFILER:
    local_scheduler_prefix = ["valgrind", "--tool=callgrind", local_scheduler_filepath]
  else:
    local_scheduler_prefix = [local_scheduler_filepath]
  local_scheduler_name = "/tmp/scheduler{}".format(random_name())
  p = subprocess.Popen(local_scheduler_prefix + ["-s", local_scheduler_name, "-r", redis_address, "-p", plasma_store_name])
  if cleanup:
    all_processes.append(p)
  return local_scheduler_name

def start_objstore(node_ip_address, redis_address, cleanup=True):
  """This method starts an object store process.

  Args:
    node_ip_address (str): The ip address of the node running the object store.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
  """
  # Let the object store use a fraction of the system memory.
  system_memory = psutil.virtual_memory().total
  plasma_store_memory = int(system_memory * 0.75)
  plasma_store_filepath = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../plasma/build/plasma_store")
  if RUN_PLASMA_STORE_PROFILER:
    plasma_store_prefix = ["valgrind", "--tool=callgrind", plasma_store_filepath]
  else:
    plasma_store_prefix = [plasma_store_filepath]
  store_name = "/tmp/ray_plasma_store{}".format(random_name())
  p1 = subprocess.Popen(plasma_store_prefix + ["-s", store_name, "-m", str(plasma_store_memory)])

  manager_name = "/tmp/ray_plasma_manager{}".format(random_name())
  p2, manager_port = plasma.start_plasma_manager(store_name, manager_name, redis_address, run_profiler=RUN_PLASMA_MANAGER_PROFILER)

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

def start_webui(redis_port, cleanup=True):
  """This method starts the web interface.

  Args:
    redis_port (int): The redis server's port
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits. This is True by default.

  """
  executable = "nodejs" if sys.platform == "linux" or sys.platform == "linux2" else "node"
  directory = "../webui"
  command = [executable, os.path.join(os.path.abspath(os.path.dirname(__file__)), directory + "/index.js"), str(redis_port)]
  with open("/tmp/webui_out.txt", "wb") as out:
    p = subprocess.Popen(command, stdout=out)
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
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workers/default_worker.py")
  # Start Redis.
  redis_port = start_redis(cleanup=True)
  redis_address = address(node_ip_address, redis_port)
  time.sleep(0.1)
  # Start Plasma.
  object_store_name, object_store_manager_name, object_store_manager_port = start_objstore(node_ip_address, redis_address, cleanup=True)
  # Start the local scheduler.
  time.sleep(0.1)
  local_scheduler_name = start_local_scheduler(redis_address, object_store_name, cleanup=True)
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
  start_webui(redis_port)
  time.sleep(0.2)
  return address_info
