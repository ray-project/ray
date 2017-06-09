from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple, OrderedDict
import os
import psutil
import random
import redis
import signal
import socket
import subprocess
import sys
import time
import threading

# Ray modules
import ray.local_scheduler
import ray.plasma
import ray.global_scheduler as global_scheduler

PROCESS_TYPE_MONITOR = "monitor"
PROCESS_TYPE_LOG_MONITOR = "log_monitor"
PROCESS_TYPE_WORKER = "worker"
PROCESS_TYPE_LOCAL_SCHEDULER = "local_scheduler"
PROCESS_TYPE_PLASMA_MANAGER = "plasma_manager"
PROCESS_TYPE_PLASMA_STORE = "plasma_store"
PROCESS_TYPE_GLOBAL_SCHEDULER = "global_scheduler"
PROCESS_TYPE_REDIS_SERVER = "redis_server"
PROCESS_TYPE_WEB_UI = "web_ui"

# This is a dictionary tracking all of the processes of different types that
# have been started by this services module. Note that the order of the keys is
# important because it determines the order in which these processes will be
# terminated when Ray exits, and certain orders will cause errors to be logged
# to the screen.
all_processes = OrderedDict([(PROCESS_TYPE_MONITOR, []),
                             (PROCESS_TYPE_LOG_MONITOR, []),
                             (PROCESS_TYPE_WORKER, []),
                             (PROCESS_TYPE_LOCAL_SCHEDULER, []),
                             (PROCESS_TYPE_PLASMA_MANAGER, []),
                             (PROCESS_TYPE_PLASMA_STORE, []),
                             (PROCESS_TYPE_GLOBAL_SCHEDULER, []),
                             (PROCESS_TYPE_REDIS_SERVER, []),
                             (PROCESS_TYPE_WEB_UI, [])],)

# True if processes are run in the valgrind profiler.
RUN_LOCAL_SCHEDULER_PROFILER = False
RUN_PLASMA_MANAGER_PROFILER = False
RUN_PLASMA_STORE_PROFILER = False

# ObjectStoreAddress tuples contain all information necessary to connect to an
# object store. The fields are:
# - name: The socket name for the object store
# - manager_name: The socket name for the object store manager
# - manager_port: The Internet port that the object store manager listens on
ObjectStoreAddress = namedtuple("ObjectStoreAddress", ["name",
                                                       "manager_name",
                                                       "manager_port"])


def address(ip_address, port):
  return ip_address + ":" + str(port)


def get_ip_address(address):
  try:
    ip_address = address.split(":")[0]
  except:
    raise Exception("Unable to parse IP address from address "
                    "{}".format(address))
  return ip_address


def get_port(address):
  try:
    port = int(address.split(":")[1])
  except:
    raise Exception("Unable to parse port from address {}".format(address))
  return port


def new_port():
  return random.randint(10000, 65535)


def random_name():
  return str(random.randint(0, 99999999))


def kill_process(p):
  """Kill a process.

  Args:
    p: The process to kill.

  Returns:
    True if the process was killed successfully and false otherwise.
  """
  if p.poll() is not None:
    # The process has already terminated.
    return True
  if any([RUN_LOCAL_SCHEDULER_PROFILER, RUN_PLASMA_MANAGER_PROFILER,
          RUN_PLASMA_STORE_PROFILER]):
    # Give process signal to write profiler data.
    os.kill(p.pid, signal.SIGINT)
    # Wait for profiling data to be written.
    time.sleep(0.1)

  # Allow the process one second to exit gracefully.
  p.terminate()
  timer = threading.Timer(1, lambda p: p.kill(), [p])
  try:
    timer.start()
    p.wait()
  finally:
    timer.cancel()

  if p.poll() is not None:
    return True

  # If the process did not exit within one second, force kill it.
  p.kill()
  if p.poll() is not None:
    return True

  # The process was not killed for some reason.
  return False


def cleanup():
  """When running in local mode, shutdown the Ray processes.

  This method is used to shutdown processes that were started with
  services.start_ray_head(). It kills all scheduler, object store, and worker
  processes that were started by this services module. Driver processes are
  started and disconnected by worker.py.
  """
  successfully_shut_down = True
  # Terminate the processes in reverse order.
  for process_type in all_processes.keys():
    # Kill all of the processes of a certain type.
    for p in all_processes[process_type]:
      success = kill_process(p)
      successfully_shut_down = successfully_shut_down and success
    # Reset the list of processes of this type.
    all_processes[process_type] = []
  if not successfully_shut_down:
    print("Ray did not shut down properly.")


def all_processes_alive(exclude=[]):
  """Check if all of the processes are still alive.

  Args:
    exclude: Don't check the processes whose types are in this list.
  """
  for process_type, processes in all_processes.items():
    # Note that p.poll() returns the exit code that the process exited with, so
    # an exit code of None indicates that the process is still alive.
    processes_alive = [p.poll() is None for p in processes]
    if (not all(processes_alive) and process_type not in exclude):
      print("A process of type {} has died.".format(process_type))
      return False
  return True


def get_node_ip_address(address="8.8.8.8:53"):
  """Determine the IP address of the local node.

  Args:
    address (str): The IP address and port of any known live service on the
      network you care about.

  Returns:
    The IP address of the current node.
  """
  ip_address, port = address.split(":")
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.connect((ip_address, int(port)))
  return s.getsockname()[0]


def record_log_files_in_redis(redis_address, node_ip_address, log_files):
  """Record in Redis that a new log file has been created.

  This is used so that each log monitor can check Redis and figure out which
  log files it is reponsible for monitoring.

  Args:
    redis_address: The address of the redis server.
    node_ip_address: The IP address of the node that the log file exists on.
    log_files: A list of file handles for the log files. If one of the file
      handles is None, we ignore it.
  """
  for log_file in log_files:
    if log_file is not None:
      redis_ip_address, redis_port = redis_address.split(":")
      redis_client = redis.StrictRedis(host=redis_ip_address, port=redis_port)
      # The name of the key storing the list of log filenames for this IP
      # address.
      log_file_list_key = "LOG_FILENAMES:{}".format(node_ip_address)
      redis_client.rpush(log_file_list_key, log_file.name)


def wait_for_redis_to_start(redis_ip_address, redis_port, num_retries=5):
  """Wait for a Redis server to be available.

  This is accomplished by creating a Redis client and sending a random command
  to the server until the command gets through.

  Args:
    redis_ip_address (str): The IP address of the redis server.
    redis_port (int): The port of the redis server.
    num_retries (int): The number of times to try connecting with redis. The
      client will sleep for one second between attempts.

  Raises:
    Exception: An exception is raised if we could not connect with Redis.
  """
  redis_client = redis.StrictRedis(host=redis_ip_address, port=redis_port)
  # Wait for the Redis server to start.
  counter = 0
  while counter < num_retries:
    try:
      # Run some random command and see if it worked.
      print("Waiting for redis server at {}:{} to respond..."
            .format(redis_ip_address, redis_port))
      redis_client.client_list()
    except redis.ConnectionError as e:
      # Wait a little bit.
      time.sleep(1)
      print("Failed to connect to the redis server, retrying.")
      counter += 1
    else:
      break
  if counter == num_retries:
    raise Exception("Unable to connect to Redis. If the Redis instance is on "
                    "a different machine, check that your firewall is "
                    "configured properly.")


def start_redis(node_ip_address,
                port=None,
                num_redis_shards=1,
                redirect_output=False,
                cleanup=True):
  """Start the Redis global state store.

  Args:
    node_ip_address: The IP address of the current node. This is only used for
      recording the log filenames in Redis.
    port (int): If provided, the primary Redis shard will be started on this
      port.
    num_redis_shards (int): If provided, the number of Redis shards to start,
      in addition to the primary one. The default value is one shard.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      all Redis processes started by this method will be killed by
      serices.cleanup() when the Python process that imported services exits.

  Returns:
    A tuple of the address for the primary Redis shard and a list of addresses
    for the remaining shards.
  """
  redis_stdout_file, redis_stderr_file = new_log_files(
      "redis", redirect_output)
  assigned_port, _ = start_redis_instance(
      node_ip_address=node_ip_address, port=port,
      stdout_file=redis_stdout_file, stderr_file=redis_stderr_file,
      cleanup=cleanup)
  if port is not None:
    assert assigned_port == port
  port = assigned_port
  redis_address = address(node_ip_address, port)

  # Register the number of Redis shards in the primary shard, so that clients
  # know how many redis shards to expect under RedisShards.
  redis_client = redis.StrictRedis(host=node_ip_address, port=port)
  redis_client.set("NumRedisShards", str(num_redis_shards))

  # Start other Redis shards listening on random ports. Each Redis shard logs
  # to a separate file, prefixed by "redis-<shard number>".
  redis_shards = []
  for i in range(num_redis_shards):
    redis_stdout_file, redis_stderr_file = new_log_files(
        "redis-{}".format(i), redirect_output)
    redis_shard_port, _ = start_redis_instance(
        node_ip_address=node_ip_address, stdout_file=redis_stdout_file,
        stderr_file=redis_stderr_file, cleanup=cleanup)
    shard_address = address(node_ip_address, redis_shard_port)
    redis_shards.append(shard_address)
    # Store redis shard information in the primary redis shard.
    redis_client.rpush("RedisShards", shard_address)

  return redis_address, redis_shards


def start_redis_instance(node_ip_address="127.0.0.1",
                         port=None,
                         num_retries=20,
                         stdout_file=None,
                         stderr_file=None,
                         cleanup=True):
  """Start a single Redis server.

  Args:
    node_ip_address (str): The IP address of the current node. This is only
      used for recording the log filenames in Redis.
    port (int): If provided, start a Redis server with this port.
    num_retries (int): The number of times to attempt to start Redis. If a port
      is provided, this defaults to 1.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.

  Returns:
    A tuple of the port used by Redis and a handle to the process that was
      started. If a port is passed in, then the returned port value is the
      same.

  Raises:
    Exception: An exception is raised if Redis could not be started.
  """
  redis_filepath = os.path.join(
      os.path.dirname(os.path.abspath(__file__)),
      "./core/src/common/thirdparty/redis/src/redis-server")
  redis_module = os.path.join(
      os.path.dirname(os.path.abspath(__file__)),
      "./core/src/common/redis_module/libray_redis_module.so")
  assert os.path.isfile(redis_filepath)
  assert os.path.isfile(redis_module)
  counter = 0
  if port is not None:
    # If a port is specified, then try only once to connect.
    num_retries = 1
  else:
    port = new_port()
  while counter < num_retries:
    if counter > 0:
      print("Redis failed to start, retrying now.")
    p = subprocess.Popen([redis_filepath,
                          "--port", str(port),
                          "--loglevel", "warning",
                          "--loadmodule", redis_module],
                         stdout=stdout_file, stderr=stderr_file)
    time.sleep(0.1)
    # Check if Redis successfully started (or at least if it the executable did
    # not exit within 0.1 seconds).
    if p.poll() is None:
      if cleanup:
        all_processes[PROCESS_TYPE_REDIS_SERVER].append(p)
      break
    port = new_port()
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
  # Increase the hard and soft limits for the redis client pubsub buffer to
  # 128MB. This is a hack to make it less likely for pubsub messages to be
  # dropped and for pubsub connections to therefore be killed.
  cur_config = (redis_client.config_get("client-output-buffer-limit")
                ["client-output-buffer-limit"])
  cur_config_list = cur_config.split()
  assert len(cur_config_list) == 12
  cur_config_list[8:] = ["pubsub", "134217728", "134217728", "60"]
  redis_client.config_set("client-output-buffer-limit",
                          " ".join(cur_config_list))
  # Put a time stamp in Redis to indicate when it was started.
  redis_client.set("redis_start_time", time.time())
  # Record the log files in Redis.
  record_log_files_in_redis(address(node_ip_address, port), node_ip_address,
                            [stdout_file, stderr_file])
  return port, p


def start_log_monitor(redis_address, node_ip_address, stdout_file=None,
                      stderr_file=None, cleanup=cleanup):
  """Start a log monitor process.

  Args:
    redis_address (str): The address of the Redis instance.
    node_ip_address (str): The IP address of the node that this log monitor is
      running on.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits.
  """
  log_monitor_filepath = os.path.join(
      os.path.dirname(os.path.abspath(__file__)),
      "log_monitor.py")
  p = subprocess.Popen(["python", log_monitor_filepath,
                        "--redis-address", redis_address,
                        "--node-ip-address", node_ip_address],
                       stdout=stdout_file, stderr=stderr_file)
  if cleanup:
    all_processes[PROCESS_TYPE_LOG_MONITOR].append(p)
  record_log_files_in_redis(redis_address, node_ip_address,
                            [stdout_file, stderr_file])


def start_global_scheduler(redis_address, node_ip_address,
                           stdout_file=None, stderr_file=None, cleanup=True):
  """Start a global scheduler process.

  Args:
    redis_address (str): The address of the Redis instance.
    node_ip_address: The IP address of the node that this scheduler will run
      on.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits.
  """
  p = global_scheduler.start_global_scheduler(redis_address,
                                              node_ip_address,
                                              stdout_file=stdout_file,
                                              stderr_file=stderr_file)
  if cleanup:
    all_processes[PROCESS_TYPE_GLOBAL_SCHEDULER].append(p)
  record_log_files_in_redis(redis_address, node_ip_address,
                            [stdout_file, stderr_file])


def start_webui(redis_address, node_ip_address, backend_stdout_file=None,
                backend_stderr_file=None, polymer_stdout_file=None,
                polymer_stderr_file=None, cleanup=True):
  """Attempt to start the Ray web UI.

  Args:
    redis_address (str): The address of the Redis server.
    node_ip_address: The IP address of the node that this process will run on.
    backend_stdout_file: A file handle opened for writing to redirect the
      backend stdout to. If no redirection should happen, then this should be
      None.
    backend_stderr_file: A file handle opened for writing to redirect the
      backend stderr to. If no redirection should happen, then this should be
      None.
    polymer_stdout_file: A file handle opened for writing to redirect the
      polymer stdout to. If no redirection should happen, then this should be
      None.
    polymer_stderr_file: A file handle opened for writing to redirect the
      polymer stderr to. If no redirection should happen, then this should be
      None.
    cleanup (bool): True if using Ray in local mode. If cleanup is True, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits.

  Return:
    True if the web UI was successfully started, otherwise false.
  """
  webui_backend_filepath = os.path.join(
      os.path.dirname(os.path.abspath(__file__)),
      "../../webui/backend/ray_ui.py")
  webui_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 "../../webui/")

  if sys.version_info >= (3, 0):
    python_executable = "python"
  else:
    # If the user is using Python 2, it is still possible to run the webserver
    # separately with Python 3, so try to find a Python 3 executable.
    try:
      python_executable = subprocess.check_output(
          ["which", "python3"]).decode("ascii").strip()
    except Exception as e:
      print("Not starting the web UI because the web UI requires Python 3.")
      return False

  backend_process = subprocess.Popen([python_executable,
                                      webui_backend_filepath,
                                      "--redis-address", redis_address],
                                     stdout=backend_stdout_file,
                                     stderr=backend_stderr_file)

  time.sleep(0.1)
  if backend_process.poll() is not None:
    # Failed to start the web UI.
    print("The web UI failed to start.")
    return False

  # Try to start polymer. If this fails, it may that port 8080 is already in
  # use. It'd be nice to test for this, but doing so by calling "bind" may
  # start using the port and prevent polymer from using it.
  try:
    polymer_process = subprocess.Popen(["polymer", "serve", "--port", "8080"],
                                       cwd=webui_directory,
                                       stdout=polymer_stdout_file,
                                       stderr=polymer_stderr_file)
  except Exception as e:
    print("Failed to start polymer.")
    # Kill the backend since it won't work without polymer.
    try:
      backend_process.kill()
    except Exception as e:
      pass
    return False

  # Unfortunately this block of code is unlikely to catch any problems because
  # when polymer throws an error on startup, it is typically after several
  # seconds.
  time.sleep(0.1)
  if polymer_process.poll() is not None:
    # Failed to start polymer.
    print("Failed to serve the web UI with polymer.")
    # Kill the backend since it won't work without polymer.
    try:
      backend_process.kill()
    except Exception as e:
      pass
    return False

  if cleanup:
    all_processes[PROCESS_TYPE_WEB_UI].append(backend_process)
    all_processes[PROCESS_TYPE_WEB_UI].append(polymer_process)
  record_log_files_in_redis(redis_address, node_ip_address,
                            [backend_stdout_file, backend_stderr_file,
                             polymer_stdout_file, polymer_stderr_file])

  return True


def start_local_scheduler(redis_address,
                          node_ip_address,
                          plasma_store_name,
                          plasma_manager_name,
                          worker_path,
                          plasma_address=None,
                          stdout_file=None,
                          stderr_file=None,
                          cleanup=True,
                          num_cpus=None,
                          num_gpus=None,
                          num_workers=0):
  """Start a local scheduler process.

  Args:
    redis_address (str): The address of the Redis instance.
    node_ip_address (str): The IP address of the node that this local scheduler
      is running on.
    plasma_store_name (str): The name of the plasma store socket to connect to.
    plasma_manager_name (str): The name of the plasma manager socket to connect
      to.
    worker_path (str): The path of the script to use when the local scheduler
      starts up new workers.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
    num_cpus: The number of CPUs the local scheduler should be configured with.
    num_gpus: The number of GPUs the local scheduler should be configured with.
    num_workers (int): The number of workers that the local scheduler should
      start.

  Return:
    The name of the local scheduler socket.
  """
  if num_cpus is None:
    # By default, use the number of hardware execution threads for the number
    # of cores.
    num_cpus = psutil.cpu_count()
  if num_gpus is None:
    # By default, assume this node has no GPUs.
    num_gpus = 0
  print("Starting local scheduler with {} CPUs and {} GPUs.".format(num_cpus,
                                                                    num_gpus))
  local_scheduler_name, p = ray.local_scheduler.start_local_scheduler(
      plasma_store_name,
      plasma_manager_name,
      worker_path=worker_path,
      node_ip_address=node_ip_address,
      redis_address=redis_address,
      plasma_address=plasma_address,
      use_profiler=RUN_LOCAL_SCHEDULER_PROFILER,
      stdout_file=stdout_file,
      stderr_file=stderr_file,
      static_resource_list=[num_cpus, num_gpus],
      num_workers=num_workers)
  if cleanup:
    all_processes[PROCESS_TYPE_LOCAL_SCHEDULER].append(p)
  record_log_files_in_redis(redis_address, node_ip_address,
                            [stdout_file, stderr_file])
  return local_scheduler_name


def start_objstore(node_ip_address, redis_address,
                   object_manager_port=None, store_stdout_file=None,
                   store_stderr_file=None, manager_stdout_file=None,
                   manager_stderr_file=None, cleanup=True,
                   objstore_memory=None):
  """This method starts an object store process.

  Args:
    node_ip_address (str): The IP address of the node running the object store.
    redis_address (str): The address of the Redis instance to connect to.
    object_manager_port (int): The port to use for the object manager. If this
      is not provided, one will be generated randomly.
    store_stdout_file: A file handle opened for writing to redirect stdout to.
      If no redirection should happen, then this should be None.
    store_stderr_file: A file handle opened for writing to redirect stderr to.
      If no redirection should happen, then this should be None.
    manager_stdout_file: A file handle opened for writing to redirect stdout
      to. If no redirection should happen, then this should be None.
    manager_stderr_file: A file handle opened for writing to redirect stderr
      to. If no redirection should happen, then this should be None.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
    objstore_memory: The amount of memory (in bytes) to start the object store
      with.

  Return:
    A tuple of the Plasma store socket name, the Plasma manager socket name,
      and the plasma manager port.
  """
  if objstore_memory is None:
    # Compute a fraction of the system memory for the Plasma store to use.
    system_memory = psutil.virtual_memory().total
    if sys.platform == "linux" or sys.platform == "linux2":
      # On linux we use /dev/shm, its size is half the size of the physical
      # memory. To not overflow it, we set the plasma memory limit to 0.4 times
      # the size of the physical memory.
      objstore_memory = int(system_memory * 0.4)
      # Compare the requested memory size to the memory available in /dev/shm.
      shm_fd = os.open("/dev/shm", os.O_RDONLY)
      try:
        shm_fs_stats = os.fstatvfs(shm_fd)
        # The value shm_fs_stats.f_bsize is the block size and the value
        # shm_fs_stats.f_bavail is the number of available blocks.
        shm_avail = shm_fs_stats.f_bsize * shm_fs_stats.f_bavail
        if objstore_memory > shm_avail:
          print("Warning: Reducing object store memory because /dev/shm has "
                "only {} bytes available. You may be able to free up space by "
                "deleting files in /dev/shm. If you are inside a Docker "
                "container, you may need to pass an argument with the flag "
                "'--shm-size' to 'docker run'.".format(shm_avail))
          objstore_memory = int(shm_avail * 0.8)
      finally:
        os.close(shm_fd)
    else:
      objstore_memory = int(system_memory * 0.8)
  # Start the Plasma store.
  plasma_store_name, p1 = ray.plasma.start_plasma_store(
      plasma_store_memory=objstore_memory,
      use_profiler=RUN_PLASMA_STORE_PROFILER,
      stdout_file=store_stdout_file,
      stderr_file=store_stderr_file)
  # Start the plasma manager.
  if object_manager_port is not None:
    (plasma_manager_name, p2,
     plasma_manager_port) = ray.plasma.start_plasma_manager(
        plasma_store_name,
        redis_address,
        plasma_manager_port=object_manager_port,
        node_ip_address=node_ip_address,
        num_retries=1,
        run_profiler=RUN_PLASMA_MANAGER_PROFILER,
        stdout_file=manager_stdout_file,
        stderr_file=manager_stderr_file)
    assert plasma_manager_port == object_manager_port
  else:
    (plasma_manager_name, p2,
     plasma_manager_port) = ray.plasma.start_plasma_manager(
        plasma_store_name,
        redis_address,
        node_ip_address=node_ip_address,
        run_profiler=RUN_PLASMA_MANAGER_PROFILER,
        stdout_file=manager_stdout_file,
        stderr_file=manager_stderr_file)
  if cleanup:
    all_processes[PROCESS_TYPE_PLASMA_STORE].append(p1)
    all_processes[PROCESS_TYPE_PLASMA_MANAGER].append(p2)
  record_log_files_in_redis(redis_address, node_ip_address,
                            [store_stdout_file, store_stderr_file,
                             manager_stdout_file, manager_stderr_file])

  return ObjectStoreAddress(plasma_store_name, plasma_manager_name,
                            plasma_manager_port)


def start_worker(node_ip_address, object_store_name, object_store_manager_name,
                 local_scheduler_name, redis_address, worker_path,
                 stdout_file=None, stderr_file=None, cleanup=True):
  """This method starts a worker process.

  Args:
    node_ip_address (str): The IP address of the node that this worker is
      running on.
    object_store_name (str): The name of the object store.
    object_store_manager_name (str): The name of the object store manager.
    local_scheduler_name (str): The name of the local scheduler.
    redis_address (str): The address that the Redis server is listening on.
    worker_path (str): The path of the source code which the worker process
      will run.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits. This is True by default.
  """
  command = ["python",
             worker_path,
             "--node-ip-address=" + node_ip_address,
             "--object-store-name=" + object_store_name,
             "--object-store-manager-name=" + object_store_manager_name,
             "--local-scheduler-name=" + local_scheduler_name,
             "--redis-address=" + str(redis_address)]
  p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
  if cleanup:
    all_processes[PROCESS_TYPE_WORKER].append(p)
  record_log_files_in_redis(redis_address, node_ip_address,
                            [stdout_file, stderr_file])


def start_monitor(redis_address, node_ip_address, stdout_file=None,
                  stderr_file=None, cleanup=True):
  """Run a process to monitor the other processes.

  Args:
    redis_address (str): The address that the Redis server is listening on.
    node_ip_address: The IP address of the node that this process will run on.
    stdout_file: A file handle opened for writing to redirect stdout to. If no
      redirection should happen, then this should be None.
    stderr_file: A file handle opened for writing to redirect stderr to. If no
      redirection should happen, then this should be None.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by services.cleanup() when the Python process
      that imported services exits. This is True by default.
  """
  monitor_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "monitor.py")
  command = ["python",
             monitor_path,
             "--redis-address=" + str(redis_address)]
  p = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
  if cleanup:
    all_processes[PROCESS_TYPE_WORKER].append(p)
  record_log_files_in_redis(redis_address, node_ip_address,
                            [stdout_file, stderr_file])


def start_ray_processes(address_info=None,
                        node_ip_address="127.0.0.1",
                        redis_port=None,
                        num_workers=None,
                        num_local_schedulers=1,
                        num_redis_shards=1,
                        worker_path=None,
                        cleanup=True,
                        redirect_output=False,
                        include_global_scheduler=False,
                        include_log_monitor=False,
                        include_webui=False,
                        start_workers_from_local_scheduler=True,
                        num_cpus=None,
                        num_gpus=None):
  """Helper method to start Ray processes.

  Args:
    address_info (dict): A dictionary with address information for processes
      that have already been started. If provided, address_info will be
      modified to include processes that are newly started.
    node_ip_address (str): The IP address of this node.
    redis_port (int): The port that the primary Redis shard should listen to.
      If None, then a random port will be chosen. If the key "redis_address" is
      in address_info, then this argument will be ignored.
    num_workers (int): The number of workers to start.
    num_local_schedulers (int): The total number of local schedulers required.
      This is also the total number of object stores required. This method will
      start new instances of local schedulers and object stores until there are
      num_local_schedulers existing instances of each, including ones already
      registered with the given address_info.
    num_redis_shards: The number of Redis shards to start in addition to the
      primary Redis shard.
    worker_path (str): The path of the source code that will be run by the
      worker.
    cleanup (bool): If cleanup is true, then the processes started here will be
      killed by services.cleanup() when the Python process that called this
      method exits.
    redirect_output (bool): True if stdout and stderr should be redirected to a
      file.
    include_global_scheduler (bool): If include_global_scheduler is True, then
      start a global scheduler process.
    include_log_monitor (bool): If True, then start a log monitor to monitor
      the log files for all processes on this node and push their contents to
      Redis.
    include_webui (bool): If True, then attempt to start the web UI. Note that
      this is only possible with Python 3.
    start_workers_from_local_scheduler (bool): If this flag is True, then start
      the initial workers from the local scheduler. Else, start them from
      Python.
    num_cpus: A list of length num_local_schedulers containing the number of
      CPUs each local scheduler should be configured with.
    num_gpus: A list of length num_local_schedulers containing the number of
      GPUs each local scheduler should be configured with.

  Returns:
    A dictionary of the address information for the processes that were
      started.
  """
  if not isinstance(num_cpus, list):
    num_cpus = num_local_schedulers * [num_cpus]
  if not isinstance(num_gpus, list):
    num_gpus = num_local_schedulers * [num_gpus]
  assert len(num_cpus) == num_local_schedulers
  assert len(num_gpus) == num_local_schedulers

  if num_workers is not None:
    workers_per_local_scheduler = num_local_schedulers * [num_workers]
  else:
    workers_per_local_scheduler = []
    for cpus in num_cpus:
      workers_per_local_scheduler.append(cpus if cpus is not None
                                         else psutil.cpu_count())

  if address_info is None:
    address_info = {}
  address_info["node_ip_address"] = node_ip_address

  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "workers/default_worker.py")

  # Start Redis if there isn't already an instance running. TODO(rkn): We are
  # suppressing the output of Redis because on Linux it prints a bunch of
  # warning messages when it starts up. Instead of suppressing the output, we
  # should address the warnings.
  redis_address = address_info.get("redis_address")
  redis_shards = address_info.get("redis_shards", [])
  if redis_address is None:
    redis_address, redis_shards = start_redis(
        node_ip_address, port=redis_port, num_redis_shards=num_redis_shards,
        redirect_output=redirect_output, cleanup=cleanup)
    address_info["redis_address"] = redis_address
    time.sleep(0.1)

    # Start monitoring the processes.
    monitor_stdout_file, monitor_stderr_file = new_log_files("monitor",
                                                             redirect_output)
    start_monitor(redis_address,
                  node_ip_address,
                  stdout_file=monitor_stdout_file,
                  stderr_file=monitor_stderr_file)

  if redis_shards == []:
    # Get redis shards from primary redis instance.
    redis_ip_address, redis_port = redis_address.split(":")
    redis_client = redis.StrictRedis(host=redis_ip_address, port=redis_port)
    redis_shards = redis_client.lrange("RedisShards", start=0, end=-1)
    redis_shards = [shard.decode("ascii") for shard in redis_shards]
    address_info["redis_shards"] = redis_shards

  # Start the log monitor, if necessary.
  if include_log_monitor:
    log_monitor_stdout_file, log_monitor_stderr_file = new_log_files(
        "log_monitor", redirect_output=True)
    start_log_monitor(redis_address,
                      node_ip_address,
                      stdout_file=log_monitor_stdout_file,
                      stderr_file=log_monitor_stderr_file,
                      cleanup=cleanup)

  # Start the global scheduler, if necessary.
  if include_global_scheduler:
    global_scheduler_stdout_file, global_scheduler_stderr_file = new_log_files(
        "global_scheduler", redirect_output)
    start_global_scheduler(redis_address,
                           node_ip_address,
                           stdout_file=global_scheduler_stdout_file,
                           stderr_file=global_scheduler_stderr_file,
                           cleanup=cleanup)

  # Initialize with existing services.
  if "object_store_addresses" not in address_info:
    address_info["object_store_addresses"] = []
  object_store_addresses = address_info["object_store_addresses"]
  if "local_scheduler_socket_names" not in address_info:
    address_info["local_scheduler_socket_names"] = []
  local_scheduler_socket_names = address_info["local_scheduler_socket_names"]

  # Get the ports to use for the object managers if any are provided.
  object_manager_ports = (address_info["object_manager_ports"]
                          if "object_manager_ports" in address_info else None)
  if not isinstance(object_manager_ports, list):
    object_manager_ports = num_local_schedulers * [object_manager_ports]
  assert len(object_manager_ports) == num_local_schedulers

  # Start any object stores that do not yet exist.
  for i in range(num_local_schedulers - len(object_store_addresses)):
    # Start Plasma.
    plasma_store_stdout_file, plasma_store_stderr_file = new_log_files(
        "plasma_store_{}".format(i), redirect_output)
    plasma_manager_stdout_file, plasma_manager_stderr_file = new_log_files(
        "plasma_manager_{}".format(i), redirect_output)
    object_store_address = start_objstore(
        node_ip_address,
        redis_address,
        object_manager_port=object_manager_ports[i],
        store_stdout_file=plasma_store_stdout_file,
        store_stderr_file=plasma_store_stderr_file,
        manager_stdout_file=plasma_manager_stdout_file,
        manager_stderr_file=plasma_manager_stderr_file,
        cleanup=cleanup)
    object_store_addresses.append(object_store_address)
    time.sleep(0.1)

  # Start any local schedulers that do not yet exist.
  for i in range(len(local_scheduler_socket_names), num_local_schedulers):
    # Connect the local scheduler to the object store at the same index.
    object_store_address = object_store_addresses[i]
    plasma_address = "{}:{}".format(node_ip_address,
                                    object_store_address.manager_port)
    # Determine how many workers this local scheduler should start.
    if start_workers_from_local_scheduler:
      num_local_scheduler_workers = workers_per_local_scheduler[i]
      workers_per_local_scheduler[i] = 0
    else:
      # If we're starting the workers from Python, the local scheduler should
      # not start any workers.
      num_local_scheduler_workers = 0
    # Start the local scheduler.
    local_scheduler_stdout_file, local_scheduler_stderr_file = new_log_files(
        "local_scheduler_{}".format(i), redirect_output)
    local_scheduler_name = start_local_scheduler(
        redis_address,
        node_ip_address,
        object_store_address.name,
        object_store_address.manager_name,
        worker_path,
        plasma_address=plasma_address,
        stdout_file=local_scheduler_stdout_file,
        stderr_file=local_scheduler_stderr_file,
        cleanup=cleanup,
        num_cpus=num_cpus[i],
        num_gpus=num_gpus[i],
        num_workers=num_local_scheduler_workers)
    local_scheduler_socket_names.append(local_scheduler_name)
    time.sleep(0.1)

  # Make sure that we have exactly num_local_schedulers instances of object
  # stores and local schedulers.
  assert len(object_store_addresses) == num_local_schedulers
  assert len(local_scheduler_socket_names) == num_local_schedulers

  # Start any workers that the local scheduler has not already started.
  for i, num_local_scheduler_workers in enumerate(workers_per_local_scheduler):
    object_store_address = object_store_addresses[i]
    local_scheduler_name = local_scheduler_socket_names[i]
    for j in range(num_local_scheduler_workers):
      worker_stdout_file, worker_stderr_file = new_log_files(
          "worker_{}_{}".format(i, j), redirect_output)
      start_worker(node_ip_address,
                   object_store_address.name,
                   object_store_address.manager_name,
                   local_scheduler_name,
                   redis_address,
                   worker_path,
                   stdout_file=worker_stdout_file,
                   stderr_file=worker_stderr_file,
                   cleanup=cleanup)
      workers_per_local_scheduler[i] -= 1

  # Make sure that we've started all the workers.
  assert(sum(workers_per_local_scheduler) == 0)

  # Try to start the web UI.
  if include_webui:
    backend_stdout_file, backend_stderr_file = new_log_files(
        "webui_backend", redirect_output=True)
    polymer_stdout_file, polymer_stderr_file = new_log_files(
        "webui_polymer", redirect_output=True)
    successfully_started = start_webui(redis_address,
                                       node_ip_address,
                                       backend_stdout_file=backend_stdout_file,
                                       backend_stderr_file=backend_stderr_file,
                                       polymer_stdout_file=polymer_stdout_file,
                                       polymer_stderr_file=polymer_stderr_file,
                                       cleanup=cleanup)

    if successfully_started:
      print("View the web UI at http://localhost:8080.")

  # Return the addresses of the relevant processes.
  return address_info


def start_ray_node(node_ip_address,
                   redis_address,
                   object_manager_ports=None,
                   num_workers=0,
                   num_local_schedulers=1,
                   worker_path=None,
                   cleanup=True,
                   redirect_output=False,
                   num_cpus=None,
                   num_gpus=None):
  """Start the Ray processes for a single node.

  This assumes that the Ray processes on some master node have already been
  started.

  Args:
    node_ip_address (str): The IP address of this node.
    redis_address (str): The address of the Redis server.
    object_manager_ports (list): A list of the ports to use for the object
      managers. There should be one per object manager being started on this
      node (typically just one).
    num_workers (int): The number of workers to start.
    num_local_schedulers (int): The number of local schedulers to start. This
      is also the number of plasma stores and plasma managers to start.
    worker_path (str): The path of the source code that will be run by the
      worker.
    cleanup (bool): If cleanup is true, then the processes started here will be
      killed by services.cleanup() when the Python process that called this
      method exits.
    redirect_output (bool): True if stdout and stderr should be redirected to a
      file.

  Returns:
    A dictionary of the address information for the processes that were
      started.
  """
  address_info = {"redis_address": redis_address,
                  "object_manager_ports": object_manager_ports}
  return start_ray_processes(address_info=address_info,
                             node_ip_address=node_ip_address,
                             num_workers=num_workers,
                             num_local_schedulers=num_local_schedulers,
                             worker_path=worker_path,
                             include_log_monitor=True,
                             cleanup=cleanup,
                             redirect_output=redirect_output,
                             num_cpus=num_cpus,
                             num_gpus=num_gpus)


def start_ray_head(address_info=None,
                   node_ip_address="127.0.0.1",
                   redis_port=None,
                   num_workers=0,
                   num_local_schedulers=1,
                   worker_path=None,
                   cleanup=True,
                   redirect_output=False,
                   start_workers_from_local_scheduler=True,
                   num_cpus=None,
                   num_gpus=None,
                   num_redis_shards=None):
  """Start Ray in local mode.

  Args:
    address_info (dict): A dictionary with address information for processes
      that have already been started. If provided, address_info will be
      modified to include processes that are newly started.
    node_ip_address (str): The IP address of this node.
    redis_port (int): The port that the primary Redis shard should listen to.
      If None, then a random port will be chosen. If the key "redis_address" is
      in address_info, then this argument will be ignored.
    num_workers (int): The number of workers to start.
    num_local_schedulers (int): The total number of local schedulers required.
      This is also the total number of object stores required. This method will
      start new instances of local schedulers and object stores until there are
      at least num_local_schedulers existing instances of each, including ones
      already registered with the given address_info.
    worker_path (str): The path of the source code that will be run by the
      worker.
    cleanup (bool): If cleanup is true, then the processes started here will be
      killed by services.cleanup() when the Python process that called this
      method exits.
    redirect_output (bool): True if stdout and stderr should be redirected to a
      file.
    start_workers_from_local_scheduler (bool): If this flag is True, then start
      the initial workers from the local scheduler. Else, start them from
      Python.
    num_cpus (int): number of cpus to configure the local scheduler with.
    num_gpus (int): number of gpus to configure the local scheduler with.
    num_redis_shards: The number of Redis shards to start in addition to the
      primary Redis shard.

  Returns:
    A dictionary of the address information for the processes that were
      started.
  """
  num_redis_shards = 1 if num_redis_shards is None else num_redis_shards
  return start_ray_processes(
      address_info=address_info,
      node_ip_address=node_ip_address,
      redis_port=redis_port,
      num_workers=num_workers,
      num_local_schedulers=num_local_schedulers,
      worker_path=worker_path,
      cleanup=cleanup,
      redirect_output=redirect_output,
      include_global_scheduler=True,
      include_log_monitor=True,
      include_webui=False,
      start_workers_from_local_scheduler=start_workers_from_local_scheduler,
      num_cpus=num_cpus,
      num_gpus=num_gpus,
      num_redis_shards=num_redis_shards)


def new_log_files(name, redirect_output):
  """Generate partially randomized filenames for log files.

  Args:
    name (str): descriptive string for this log file.
    redirect_output (bool): True if files should be generated for logging
      stdout and stderr and false if stdout and stderr should not be
      redirected.

  Returns:
    If redirect_output is true, this will return a tuple of two filehandles.
      The first is for redirecting stdout and the second is for redirecting
      stderr. If redirect_output is false, this will return a tuple of two None
      objects.
  """
  if not redirect_output:
    return None, None

  logs_dir = "/tmp/raylogs"
  if not os.path.exists(logs_dir):
    try:
      os.makedirs(logs_dir)
    except OSError as e:
      if e.errno != os.errno.EEXIST:
        raise e
      print("Attempted to create '/tmp/raylogs', but the directory already "
            "exists.")
    # Change the log directory permissions so others can use it. This is
    # important when multiple people are using the same machine.
    os.chmod(logs_dir, 0o0777)
  log_id = random.randint(0, 100000)
  log_stdout = "{}/{}-{:06d}.out".format(logs_dir, name, log_id)
  log_stderr = "{}/{}-{:06d}.err".format(logs_dir, name, log_id)
  log_stdout_file = open(log_stdout, "a")
  log_stderr_file = open(log_stderr, "a")
  return log_stdout_file, log_stderr_file
