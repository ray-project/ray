import os
import sys
import time
import subprocess32 as subprocess

# Ray modules
import config

_services_env = os.environ.copy()
_services_env["PATH"] = os.pathsep.join([os.path.dirname(os.path.abspath(__file__)), _services_env["PATH"]])

# all_processes is a list of the scheduler, object store, and worker processes
# that have been started by this services module if Ray is being used in local
# mode.
all_processes = []

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

driver_port_counter = 0
def new_driver_port():
  global driver_port_counter
  driver_port_counter += 1
  return 30000 + driver_port_counter

objstore_port_counter = 0
def new_objstore_port():
  global objstore_port_counter
  objstore_port_counter += 1
  return 20000 + objstore_port_counter

def cleanup():
  """When running in local mode, shutdown the Ray processes.

  This method is used to shutdown processes that were started with
  services.start_ray_local(). It kills all scheduler, object store, and worker
  processes that were started by this services module. Driver processes are
  started and disconnected by worker.py.
  """
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

def start_scheduler(scheduler_address, local):
  """This method starts a scheduler process.

  Args:
    scheduler_address (str): The ip address and port to use for the scheduler.
    local (bool): True if using Ray in local mode. If local is true, then this
      process will be killed by serices.cleanup() when the Python process that
      imported services exits.
  """
  p = subprocess.Popen(["scheduler", scheduler_address, "--log-file-name", config.get_log_file_path("scheduler.log")], env=_services_env)
  if local:
    all_processes.append((p, scheduler_address))

def start_objstore(scheduler_address, objstore_address, local):
  """This method starts an object store process.

  Args:
    scheduler_address (str): The ip address and port of the scheduler to connect
      to.
    objstore_address (str): The ip address and port to use for the object store.
    local (bool): True if using Ray in local mode. If local is true, then this
      process will be killed by serices.cleanup() when the Python process that
      imported services exits.
  """
  p = subprocess.Popen(["objstore", scheduler_address, objstore_address, "--log-file-name", config.get_log_file_path("-".join(["objstore", objstore_address]) + ".log")], env=_services_env)
  if local:
    all_processes.append((p, objstore_address))

def start_worker(worker_path, scheduler_address, objstore_address, worker_address, local, user_source_directory=None):
  """This method starts a worker process.

  Args:
    worker_path (str): The path of the source code which the worker process will
      run.
    scheduler_address (str): The ip address and port of the scheduler to connect
      to.
    objstore_address (str): The ip address and port of the object store to
      connect to.
    worker_address (str): The ip address and port to use for the worker.
    local (bool): True if using Ray in local mode. If local is true, then this
      process will be killed by serices.cleanup() when the Python process that
      imported services exits.
    user_source_directory (str): The directory containing the application code.
      This directory will be added to the path of each worker. If not provided,
      the directory of the script currently being run is used.
  """
  if user_source_directory is None:
    # This extracts the directory of the script that is currently being run.
    # This will allow users to import modules contained in this directory.
    user_source_directory = os.path.dirname(os.path.abspath(os.path.join(os.path.curdir, sys.argv[0])))
  p = subprocess.Popen(["python",
                        worker_path,
                        "--user-source-directory=" + user_source_directory,
                        "--scheduler-address=" + scheduler_address,
                        "--objstore-address=" + objstore_address,
                        "--worker-address=" + worker_address])
  if local:
    all_processes.append((p, worker_address))

def start_node(scheduler_address, node_ip_address, num_workers, worker_path=None, user_source_directory=None):
  """Start an object store and associated workers in the cluster setting.

  This starts an object store and the associated workers when Ray is being used
  in the cluster setting. This assumes the scheduler has already been started.

  Args:
    scheduler_address (str): ip address and port of the scheduler (which may run
      on a different node)
    node_ip_address (str): ip address (without port) of the node this function
      is run on
    num_workers (int): the number of workers to be started on this node
    worker_path (str): path of the Python worker script that will be run on the worker
    user_source_directory (str): path to the user's code the workers will import
      modules from
  """
  objstore_address = address(node_ip_address, new_objstore_port())
  start_objstore(scheduler_address, objstore_address, local=False)
  time.sleep(0.2)
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../scripts/default_worker.py")
  for _ in range(num_workers):
    start_worker(worker_path, scheduler_address, objstore_address, address(node_ip_address, new_worker_port()), user_source_directory=user_source_directory, local=False)
  time.sleep(0.5)

def start_workers(scheduler_address, objstore_address, num_workers, worker_path):
  """Start a new set of workers on this node.

  Start a new set of workers on this node. This assumes that the scheduler is
  already running and that the object store on this node is already running. The
  intended use case is that a developer wants to update the code running on the
  worker processes so first kills all of the workers and then runs this method.

  Args:
    scheduler_address (str): ip address and port of the scheduler (which may run
      on a different node)
    objstore_address (str): ip address and port of the object store (which runs
      on the same node)
    num_workers (int): the number of workers to be started on this node
    worker_path (str): path of the source code that will be run on the worker
  """
  node_ip_address = objstore_address.split(":")[0]
  for _ in range(num_workers):
    start_worker(worker_path, scheduler_address, objstore_address, address(node_ip_address, new_worker_port()), local=False)

def start_ray_local(num_objstores=1, num_workers=0, worker_path=None):
  """Start Ray in local mode.

  This method starts Ray in local mode (as opposed to cluster mode, which is
  handled by cluster.py).

  Args:
    num_objstores (int): The number of object stores to start. Aside from
      testing, this should be one.
    num_workers (int): The number of workers to start.
    worker_path (str): The path of the source code that will be run by the
      worker.

  Returns:
    The address of the scheduler, the addresses of all of the object stores, and
      the one new driver address for each object store.
  """
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../scripts/default_worker.py")
  if num_workers > 0 and num_objstores < 1:
    raise Exception("Attempting to start a cluster with {} workers per object store, but `num_objstores` is {}.".format(num_objstores))
  scheduler_address = address(IP_ADDRESS, new_scheduler_port())
  start_scheduler(scheduler_address, local=True)
  time.sleep(0.1)
  objstore_addresses = []
  # create objstores
  for i in range(num_objstores):
    objstore_address = address(IP_ADDRESS, new_objstore_port())
    objstore_addresses.append(objstore_address)
    start_objstore(scheduler_address, objstore_address, local=True)
    time.sleep(0.2)
    if i < num_objstores - 1:
      num_workers_to_start = num_workers / num_objstores
    else:
      # In case num_workers is not divisible by num_objstores, start the correct
      # remaining number of workers.
      num_workers_to_start = num_workers - (num_objstores - 1) * (num_workers / num_objstores)
    for _ in range(num_workers_to_start):
      start_worker(worker_path, scheduler_address, objstore_address, address(IP_ADDRESS, new_worker_port()), local=True)
    time.sleep(0.3)

  driver_addresses = [address(IP_ADDRESS, new_driver_port()) for _ in range(num_objstores)]
  return scheduler_address, objstore_addresses, driver_addresses
