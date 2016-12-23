from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import psutil
import os
import random
import redis
import signal
import string
import subprocess
import sys
import time

# Ray modules
import photon
import plasma
import global_scheduler

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
    if len(all_processes) > 0:
      print("Successfully shut down Ray.")
  else:
    print("Ray did not shut down properly.")
  all_processes = []

def all_processes_alive():
  return all([p.poll() is None for p in all_processes])

def wait_for_redis_to_start(redis_host, redis_port, num_retries=5):
  """Wait for a Redis server to be available.

  This is accomplished by creating a Redis client and sending a random command
  to the server until the command gets through.

  Args:
    redis_host (str): The IP address of the redis server.
    redis_port (int): The port of the redis server.
    num_retries (int): The number of times to try connecting with redis. The
      client will sleep for one second between attempts.

  Raises:
    Exception: An exception is raised if we could not connect with Redis.
  """
  redis_client = redis.StrictRedis(host=redis_host, port=redis_port)
  # Wait for the Redis server to start.
  counter = 0
  while counter < num_retries:
    try:
      # Run some random command and see if it worked.
      redis_client.client_list()
    except redis.ConnectionError as e:
      # Wait a little bit.
      time.sleep(1)
      print("Failed to connect to the redis server, retrying.")
      counter += 1
    else:
      break
  if counter == num_retries:
    raise Exception("Unable to connect to Redis. If the Redis instance is on a different machine, check that your firewall is configured properly.")

def start_redis(num_retries=20, cleanup=True, redirect_output=False):
  """Start a Redis server.

  Args:
    num_retries (int): The number of times to attempt to start Redis.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.

  Returns:
    The port used by Redis.

  Raises:
    Exception: An exception is raised if Redis could not be started.
  """
  redis_filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../common/thirdparty/redis/src/redis-server")
  redis_module = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../common/redis_module/ray_redis_module.so")
  assert os.path.isfile(redis_filepath)
  assert os.path.isfile(redis_module)
  counter = 0
  while counter < num_retries:
    if counter > 0:
      print("Redis failed to start, retrying now.")
    port = new_port()
    with open(os.devnull, "w") as FNULL:
      stdout = FNULL if redirect_output else None
      stderr = FNULL if redirect_output else None
      p = subprocess.Popen([redis_filepath, "--port", str(port), "--loglevel", "warning", "--loadmodule", redis_module], stdout=stdout, stderr=stderr)
    time.sleep(0.1)
    # Check if Redis successfully started (or at least if it the executable did
    # not exit within 0.1 seconds).
    if p.poll() is None:
      if cleanup:
        all_processes.append(p)
      break
    counter += 1
  if counter == num_retries:
    raise Exception("Couldn't start Redis.")

  # Create a Redis client just for configuring Redis.
  redis_client = redis.StrictRedis(host="127.0.0.1", port=port)
  # Wait for the Redis server to start.
  wait_for_redis_to_start("127.0.0.1", port)
  # Configure Redis to generate keyspace notifications. TODO(rkn): Change this
  # to only generate notifications for the export keys.
  redis_client.config_set("notify-keyspace-events", "Kl")
  # Configure Redis to not run in protected mode so that processes on other
  # hosts can connect to it. TODO(rkn): Do this in a more secure way.
  redis_client.config_set("protected-mode", "no")
  return port

def start_global_scheduler(redis_address, cleanup=True, redirect_output=False):
  """Start a global scheduler process.

  Args:
    redis_address (str): The address of the Redis instance.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.
  """
  p = global_scheduler.start_global_scheduler(redis_address, redirect_output=redirect_output)
  if cleanup:
    all_processes.append(p)

def start_local_scheduler(redis_address, node_ip_address, plasma_store_name, plasma_manager_name, plasma_address=None, cleanup=True, redirect_output=False):
  """Start a local scheduler process.

  Args:
    redis_address (str): The address of the Redis instance.
    node_ip_address (str): The IP address of the node that this local scheduler
      is running on.
    plasma_store_name (str): The name of the plasma store socket to connect to.
    plasma_manager_name (str): The name of the plasma manager socket to connect
      to.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.

  Return:
    The name of the local scheduler socket.
  """
  local_scheduler_name, p = photon.start_local_scheduler(plasma_store_name, plasma_manager_name, node_ip_address=node_ip_address, redis_address=redis_address, plasma_address=plasma_address, use_profiler=RUN_PHOTON_PROFILER, redirect_output=redirect_output)
  if cleanup:
    all_processes.append(p)
  return local_scheduler_name

def start_objstore(node_ip_address, redis_address, cleanup=True, redirect_output=False):
  """This method starts an object store process.

  Args:
    node_ip_address (str): The IP address of the node running the object store.
    redis_address (str): The address of the Redis instance to connect to.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.

  Return:
    A tuple of the Plasma store socket name, the Plasma manager socket name, and
      the plasma manager port.
  """
  # Compute a fraction of the system memory for the Plasma store to use.
  system_memory = psutil.virtual_memory().total
  if sys.platform == "linux" or sys.platform == "linux2":
    # On linux we use /dev/shm, its size is half the size of the physical
    # memory. To not overflow it, we set the plasma memory limit to 0.4 times
    # the size of the physical memory.
    plasma_store_memory = int(system_memory * 0.4)
  else:
    plasma_store_memory = int(system_memory * 0.75)
  # Start the Plasma store.
  plasma_store_name, p1 = plasma.start_plasma_store(plasma_store_memory=plasma_store_memory, use_profiler=RUN_PLASMA_STORE_PROFILER, redirect_output=redirect_output)
  # Start the plasma manager.
  plasma_manager_name, p2, plasma_manager_port = plasma.start_plasma_manager(plasma_store_name, redis_address, node_ip_address=node_ip_address, run_profiler=RUN_PLASMA_MANAGER_PROFILER, redirect_output=redirect_output)
  if cleanup:
    all_processes.append(p1)
    all_processes.append(p2)

  return plasma_store_name, plasma_manager_name, plasma_manager_port

def start_worker(node_ip_address, object_store_name, object_store_manager_name, local_scheduler_name, redis_address, worker_path, cleanup=True, redirect_output=False):
  """This method starts a worker process.

  Args:
    node_ip_address (str): The IP address of the node that this worker is
      running on.
    object_store_name (str): The name of the object store.
    object_store_manager_name (str): The name of the object store manager.
    local_scheduler_name (str): The name of the local scheduler.
    redis_address (int): The address that the Redis server is listening on.
    worker_path (str): The path of the source code which the worker process will
      run.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits. This is True by default.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.
  """
  command = ["python",
             worker_path,
             "--node-ip-address=" + node_ip_address,
             "--object-store-name=" + object_store_name,
             "--object-store-manager-name=" + object_store_manager_name,
             "--local-scheduler-name=" + local_scheduler_name,
             "--redis-address=" + str(redis_address)]
  with open(os.devnull, "w") as FNULL:
    stdout = FNULL if redirect_output else None
    stderr = FNULL if redirect_output else None
    p = subprocess.Popen(command, stdout=stdout, stderr=stderr)
  if cleanup:
    all_processes.append(p)

def start_webui(redis_port, cleanup=True, redirect_output=False):
  """This method starts the web interface.

  Args:
    redis_port (int): The redis server's port
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits. This is True by default.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.
  """
  executable = "nodejs" if sys.platform == "linux" or sys.platform == "linux2" else "node"
  command = [executable, os.path.join(os.path.abspath(os.path.dirname(__file__)), "../webui/index.js"), str(redis_port)]
  with open("/tmp/webui_out.txt", "wb") as out:
    with open(os.devnull, "w") as FNULL:
      stdout = FNULL if redirect_output else out
      stderr = FNULL if redirect_output else None
      p = subprocess.Popen(command, stdout=stdout, stderr=stderr)
  if cleanup:
    all_processes.append(p)

def start_ray_node(node_ip_address, redis_address, num_workers=0, num_local_schedulers=1, worker_path=None, cleanup=True, redirect_output=False):
  """Start the Ray processes for a single node.

  This assumes that the Ray processes on some master node have already been
  started.

  Args:
    node_ip_address (str): The IP address of this node.
    redis_address (str): The address of the Redis server.
    num_workers (int): The number of workers to start.
    num_local_schedulers (int): The number of local schedulers to start. This is
      also the number of plasma stores and plasma managers to start.
    worker_path (str): The path of the source code that will be run by the
      worker.
    cleanup (bool): If cleanup is true, then the processes started here will be
      killed by services.cleanup() when the Python process that called this
      method exits.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.
  """
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workers/default_worker.py")
  object_store_names = []
  object_store_manager_names = []
  local_scheduler_names = []
  for _ in range(num_local_schedulers):
    # Start Plasma.
    object_store_name, object_store_manager_name, object_store_manager_port = start_objstore(node_ip_address, redis_address, cleanup=cleanup, redirect_output=redirect_output)
    object_store_names.append(object_store_name)
    object_store_manager_names.append(object_store_manager_name)
    time.sleep(0.1)
    # Start the local scheduler.
    plasma_address = "{}:{}".format(node_ip_address, object_store_manager_port)
    local_scheduler_name = start_local_scheduler(redis_address, node_ip_address, object_store_name, object_store_manager_name, plasma_address=plasma_address, cleanup=cleanup, redirect_output=redirect_output)
    local_scheduler_names.append(local_scheduler_name)
    time.sleep(0.1)
  # Aggregate the address information together.
  address_info = {"node_ip_address": node_ip_address,
                  "object_store_names": object_store_names,
                  "object_store_manager_names": object_store_manager_names,
                  "local_scheduler_names": local_scheduler_names}
  # Start the workers.
  for i in range(num_workers):
    start_worker(address_info["node_ip_address"],
                 address_info["object_store_names"][i % num_local_schedulers],
                 address_info["object_store_manager_names"][i % num_local_schedulers],
                 address_info["local_scheduler_names"][i % num_local_schedulers],
                 redis_address,
                 worker_path,
                 cleanup=cleanup,
                 redirect_output=redirect_output)
  # Return the addresses of the relevant processes.
  return address_info

def start_ray_local(node_ip_address="127.0.0.1", num_workers=0, num_local_schedulers=1, worker_path=None, cleanup=True, redirect_output=False):
  """Start Ray in local mode.

  Args:
    node_ip_address (str): The IP address of this node.
    num_workers (int): The number of workers to start.
    num_local_schedulers (int): The number of local schedulers to start. This is
      also the number of plasma stores and plasma managers to start.
    worker_path (str): The path of the source code that will be run by the
      worker.
    cleanup (bool): If cleanup is true, then the processes started here will be
      killed by services.cleanup() when the Python process that called this
      method exits.
    redirect_output (bool): True if stdout and stderr should be redirected to
      /dev/null.

  Returns:
    This returns a dictionary of the address information for the processes that
      were started.
  """
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workers/default_worker.py")
  # Start Redis.
  redis_port = start_redis(cleanup=cleanup, redirect_output=redirect_output)
  redis_address = address(node_ip_address, redis_port)
  time.sleep(0.1)
  # Start the global scheduler.
  start_global_scheduler(redis_address, cleanup=cleanup, redirect_output=redirect_output)
  object_store_names = []
  object_store_manager_names = []
  local_scheduler_names = []
  for _ in range(num_local_schedulers):
    # Start Plasma.
    object_store_name, object_store_manager_name, object_store_manager_port = start_objstore(node_ip_address, redis_address, cleanup=cleanup, redirect_output=redirect_output)
    object_store_names.append(object_store_name)
    object_store_manager_names.append(object_store_manager_name)
    time.sleep(0.1)
    # Start the local scheduler.
    plasma_address = "{}:{}".format(node_ip_address, object_store_manager_port)
    local_scheduler_name = start_local_scheduler(redis_address, node_ip_address, object_store_name, object_store_manager_name, plasma_address=plasma_address, cleanup=cleanup, redirect_output=redirect_output)
    local_scheduler_names.append(local_scheduler_name)
    time.sleep(0.1)
  # Aggregate the address information together.
  address_info = {"node_ip_address": node_ip_address,
                  "redis_address": redis_address,
                  "object_store_names": object_store_names,
                  "object_store_manager_names": object_store_manager_names,
                  "local_scheduler_names": local_scheduler_names}
  # Start the workers.
  for i in range(num_workers):
    start_worker(address_info["node_ip_address"],
                 address_info["object_store_names"][i % num_local_schedulers],
                 address_info["object_store_manager_names"][i % num_local_schedulers],
                 address_info["local_scheduler_names"][i % num_local_schedulers],
                 redis_address,
                 worker_path,
                 cleanup=cleanup,
                 redirect_output=redirect_output)
  # Return the addresses of the relevant processes.
  start_webui(redis_port, cleanup=cleanup, redirect_output=redirect_output)
  return address_info
