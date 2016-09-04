import os
import sys
import time
import subprocess32 as subprocess
import string
import random

# Ray modules
import config

_services_env = os.environ.copy()
_services_env["PATH"] = os.pathsep.join([os.path.dirname(os.path.abspath(__file__)), _services_env["PATH"]])
# Make GRPC only print error messages.
_services_env["GRPC_VERBOSITY"] = "ERROR"

# all_processes is a list of the scheduler, object store, and worker processes
# that have been started by this services module if Ray is being used in local
# mode.
all_processes = []

TIMEOUT_SECONDS = 5

def address(host, port):
  return host + ":" + str(port)

def new_scheduler_port():
  return random.randint(10000, 65535)

def cleanup():
  """When running in local mode, shutdown the Ray processes.

  This method is used to shutdown processes that were started with
  services.start_ray_local(). It kills all scheduler, object store, and worker
  processes that were started by this services module. Driver processes are
  started and disconnected by worker.py.
  """
  global all_processes
  successfully_shut_down = True
  for p in all_processes:
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
    print "Successfully shut down Ray."
  else:
    print "Ray did not shut down properly."
  all_processes = []

def start_scheduler(scheduler_address, cleanup):
  """This method starts a scheduler process.

  Args:
    scheduler_address (str): The ip address and port to use for the scheduler.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
  """
  scheduler_port = scheduler_address.split(":")[1]
  p = subprocess.Popen(["scheduler", scheduler_address, "--log-file-name", config.get_log_file_path("scheduler-" + scheduler_port + ".log")], env=_services_env)
  if cleanup:
    all_processes.append(p)

def start_objstore(scheduler_address, node_ip_address, cleanup):
  """This method starts an object store process.

  Args:
    scheduler_address (str): The ip address and port of the scheduler to connect
      to.
    node_ip_address (str): The ip address of the node running the object store.
    cleanup (bool): True if using Ray in local mode. If cleanup is true, then
      this process will be killed by serices.cleanup() when the Python process
      that imported services exits.
  """
  random_string = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
  p = subprocess.Popen(["objstore", scheduler_address, node_ip_address, "--log-file-name", config.get_log_file_path("-".join(["objstore", random_string]) + ".log")], env=_services_env)
  if cleanup:
    all_processes.append(p)

def start_worker(node_ip_address, worker_path, scheduler_address, objstore_address=None, cleanup=True):
  """This method starts a worker process.

  Args:
    node_ip_address (str): The IP address of the node that the worker runs on.
    worker_path (str): The path of the source code which the worker process will
      run.
    scheduler_address (str): The ip address and port of the scheduler to connect
      to.
    objstore_address (Optional[str]): The ip address and port of the object
      store to connect to.
    cleanup (Optional[bool]): True if using Ray in local mode. If cleanup is
      true, then this process will be killed by serices.cleanup() when the
      Python process that imported services exits. This is True by default.
  """
  command = ["python",
             worker_path,
             "--node-ip-address=" + node_ip_address,
             "--scheduler-address=" + scheduler_address]
  if objstore_address is not None:
    command.append("--objstore-address=" + objstore_address)
  p = subprocess.Popen(command)
  if cleanup:
    all_processes.append(p)

def start_node(scheduler_address, node_ip_address, num_workers, worker_path=None, cleanup=False):
  """Start an object store and associated workers in the cluster setting.

  This starts an object store and the associated workers when Ray is being used
  in the cluster setting. This assumes the scheduler has already been started.

  Args:
    scheduler_address (str): IP address and port of the scheduler (which may run
      on a different node).
    node_ip_address (str): IP address (without port) of the node this function
      is run on.
    num_workers (int): The number of workers to be started on this node.
    worker_path (str): Path of the Python worker script that will be run on the
      worker.
    cleanup (bool): If cleanup is True, then the processes started by this
      command will be killed when the process that imported services exits.
  """
  start_objstore(scheduler_address, node_ip_address, cleanup=cleanup)
  time.sleep(0.2)
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../scripts/default_worker.py")
  for _ in range(num_workers):
    start_worker(node_ip_address, worker_path, scheduler_address, cleanup=cleanup)
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
    start_worker(node_ip_address, worker_path, scheduler_address, cleanup=False)

def start_ray_local(node_ip_address="127.0.0.1", num_objstores=1, num_workers=0, worker_path=None):
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
    The address of the scheduler and the addresses of all of the object stores.
  """
  if worker_path is None:
    worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../scripts/default_worker.py")
  if num_objstores < 1:
    raise Exception("`num_objstores` is {}, but should be at least 1.".format(num_objstores))
  scheduler_address = address(node_ip_address, new_scheduler_port())
  start_scheduler(scheduler_address, cleanup=True)
  time.sleep(0.1)
  # create objstores
  for i in range(num_objstores):
    start_objstore(scheduler_address, node_ip_address, cleanup=True)
    time.sleep(0.2)
    if i < num_objstores - 1:
      num_workers_to_start = num_workers / num_objstores
    else:
      # In case num_workers is not divisible by num_objstores, start the correct
      # remaining number of workers.
      num_workers_to_start = num_workers - (num_objstores - 1) * (num_workers / num_objstores)
    for _ in range(num_workers_to_start):
      start_worker(node_ip_address, worker_path, scheduler_address, cleanup=True)
    time.sleep(0.3)

  return scheduler_address
