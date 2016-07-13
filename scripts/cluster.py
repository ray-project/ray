import os
import subprocess
import socket
import argparse
import threading
import IPython
import numpy as np

parser = argparse.ArgumentParser(description="Parse information about the cluster.")
parser.add_argument("--nodes", type=str, required=True, help="Test file with node IP addresses, one line per address.")
parser.add_argument("--key-file", type=str, required=True, help="Path to the file that contains the private key.")
parser.add_argument("--username", type=str, required=True, help="User name for logging in.")
parser.add_argument("--installation-directory", type=str, required=True, help="The directory in which to install Ray.")

class RayCluster(object):
  """A class for setting up, starting, and stopping Ray on a cluster.

  Attributes:
    node_ip_addresses (List[str]): A list of the ip addresses of the nodes in
      the cluster. The first element is the head node and will host the
      scheduler process.
    username (str): The username used to ssh to nodes in the cluster.
    key_file (str): The path to the key used to ssh to nodes in the cluster.
    installation_directory (str): The path on the nodes in the cluster to the
      directory in which Ray should be installed.
  """

  def __init__(self, node_ip_addresses, username, key_file, installation_directory):
    """Initialize the RayCluster object.

    Args:
      node_ip_addresses (List[str]): A list of the ip addresses of the nodes in
        the cluster. The first element is the head node and will host the
        scheduler process.
      username (str): The username used to ssh to nodes in the cluster.
      key_file (str): The path to the key used to ssh to nodes in the cluster.
      installation_directory (str): The path on the nodes in the cluster to the
        directory in which Ray should be installed.

    Raises:
      Exception: An exception is raised by check_ip_addresses if one of the ip
        addresses is not a valid ip address.
    """
    _check_ip_addresses(node_ip_addresses)
    self.node_ip_addresses = node_ip_addresses
    self.username = username
    self.key_file = key_file
    self.installation_directory = installation_directory

  def _run_command_over_ssh(self, node_ip_address, command):
    """Run a command over ssh.

    Args:
      node_ip_address (str): The ip address of the node to ssh to.
      command (str): The command to run over ssh, currently this command is not
        allowed to have any single quotes.
    """
    if "'" in command:
      raise Exception("Commands run over ssh must not contain the single quote character. This command does: {}".format(command))
    full_command = "ssh -o StrictHostKeyChecking=no -i {} {}@{} '{}'".format(self.key_file, self.username, node_ip_address, command)
    subprocess.call([full_command], shell=True)
    print "Finished running command '{}' on {}@{}.".format(command, self.username, node_ip_address)

  def _run_parallel_functions(self, functions, inputs):
    """Run functions in parallel.

    This will run each function in functions in a separate thread. This method
    blocks until all of the functions have finished.

    Args:
      functions (List[Callable]): The functions to execute in parallel.
      inputs (List[Tuple]): The inputs to the functions.
    """
    threads = []
    for i in range(len(self.node_ip_addresses)):
      t = threading.Thread(target=functions[i], args=inputs[i])
      t.start()
      threads.append(t)
    for t in threads:
      t.join()

  def _run_command_over_ssh_on_all_nodes_in_parallel(self, commands):
    """Run a command over ssh on all nodes in the cluster in parallel.

    Args:
      commands: This is either a single command to run on every node in the
        cluster ove ssh, or it is a list of commands of the same length as
        node_ip_addresses, in which case the ith command will be run on the ith
        element of node_ip_addresses. Currently this command is not allowed to
        have any single quotes.

    Raises:
      Exception: An exception is raised if commands is not a string or is not a
        list with the same length as node_ip_addresses.
    """
    if isinstance(commands, str):
      # If there is only one command, then run this command on every node in the cluster.
      commands = len(self.node_ip_addresses) * [commands]
    if len(commands) != len(self.node_ip_addresses):
      raise Exception("The number of commands must match the number of nodes.")
    functions = []
    inputs = []
    def function(node_ip_address, command):
      self._run_command_over_ssh(node_ip_address, command)
    inputs = zip(node_ip_addresses, commands)
    self._run_parallel_functions(len(self.node_ip_addresses) * [function], inputs)
    print "Finished running commands {} on all nodes.".format(inputs)

  def install_ray(self):
    """Install Ray on every node in the cluster.

    This method will ssh to each node, clone the Ray repository, install the
    dependencies, build the third-party libraries, and build Ray.
    """
    install_ray_command = """
      sudo apt-get update &&
      sudo apt-get -y install git &&
      mkdir -p "{}" &&
      cd "{}" &&
      git clone "https://github.com/amplab/ray";
      cd ray;
      ./install-dependencies.sh;
      ./setup.sh;
      ./build.sh
    """.format(self.installation_directory, self.installation_directory)
    self._run_command_over_ssh_on_all_nodes_in_parallel(install_ray_command)

  def start_ray(self, worker_directory, num_workers_per_node=10):
    """Start Ray on a cluster.

    This method is used to start Ray on a cluster. It will ssh to the head node,
    that is, the first node in the list node_ip_addresses, and it will start the
    scheduler. Then it will ssh to each node and start an object store and some
    workers.

    Args:
      worker_directory (str): The path to the local directory containing the
        worker source code. This directory must contain a file worker.py which
        is the code run by the worker processes.
      num_workers_per_node (int): The number workers to start on each node.
    """
    # First update the worker code on the nodes.
    remote_worker_path = self._update_worker_code(worker_directory)

    scripts_directory = os.path.join(self.installation_directory, "ray/scripts")
    # Start the scheduler
    # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
    start_scheduler_command = """
      cd "{}";
      source ../setup-env.sh;
      python -c "import ray; ray.services.start_scheduler(\\\"{}:10001\\\", local=False)" > start_scheduler.out 2> start_scheduler.err < /dev/null &
    """.format(scripts_directory, self.node_ip_addresses[0])
    self._run_command_over_ssh(self.node_ip_addresses[0], start_scheduler_command)

    # Start the workers on each node
    # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
    start_workers_commands = []
    for i, node_ip_address in enumerate(self.node_ip_addresses):
      start_workers_command = """
        cd "{}";
        source ../setup-env.sh;
        python -c "import ray; ray.services.start_node(\\\"{}:10001\\\", \\\"{}\\\", {}, worker_path=\\\"{}\\\")" > start_workers.out 2> start_workers.err < /dev/null &
      """.format(scripts_directory, self.node_ip_addresses[0], self.node_ip_addresses[i], num_workers_per_node, remote_worker_path)
      start_workers_commands.append(start_workers_command)
    self._run_command_over_ssh_on_all_nodes_in_parallel(start_workers_commands)

    print "cluster started; you can start the shell on the head node with:"
    setup_env_path = os.path.join(self.installation_directory, "ray/setup-env.sh")
    shell_script_path = os.path.join(self.installation_directory, "ray/scripts/shell.py")
    print """
      source "{}";
      python "{}" --scheduler-address={}:10001 --objstore-address={}:20001 --worker-address={}:30001 --attach
    """.format(setup_env_path, shell_script_path, self.node_ip_addresses[0], self.node_ip_addresses[0], self.node_ip_addresses[0])

  def restart_workers(self, worker_directory, num_workers_per_node=10):
    """Restart the workers on the cluster.

    This method is used for restarting the workers in the cluster, for example,
    to use new application code. This is done without shutting down the
    scheduler or the object stores so that work is not thrown away. It also does
    not shut down any drivers.

    Args:
      worker_directory (str): The path to the local directory containing the
        worker source code. This directory must contain a file worker.py which
        is the code run by the worker processes.
      num_workers_per_node (int): The number workers to start on each node.
    """
    # First update the worker code on the nodes.
    remote_worker_path = self._update_worker_code(worker_directory)

    scripts_directory = os.path.join(self.installation_directory, "ray/scripts")
    head_node_ip_address = self.node_ip_addresses[0]
    scheduler_address = "{}:10001".format(head_node_ip_address) # This needs to be the address of the currently running scheduler, which was presumably created in _start_ray.
    objstore_address = "{}:20001".format(head_node_ip_address) # This needs to be the address of the currently running object store, which was presumably created in _start_ray.
    shell_address = "{}:{}".format(head_node_ip_address, np.random.randint(30000, 40000)) # This address must be currently unused. In particular, it cannot be the address of any currently running shell.

    # Kill the current workers by attaching a driver to the scheduler and calling ray.kill_workers()
    # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
    kill_workers_command = """
      cd "{}";
      source ../setup-env.sh;
      python -c "import ray; ray.connect(\\\"{}\\\", \\\"{}\\\", \\\"{}\\\", is_driver=True); ray.kill_workers()"
    """.format(scripts_directory, scheduler_address, objstore_address, shell_address)
    self._run_command_over_ssh(head_node_ip_address, kill_workers_command)

    # Start new workers on each node
    # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
    start_workers_commands = []
    for i, node_ip_address in enumerate(self.node_ip_addresses):
      start_workers_command = """
        cd "{}";
        source ../setup-env.sh;
        python -c "import ray; ray.services.start_workers(\\\"{}:10001\\\", \\\"{}:20001\\\", {}, worker_path=\\\"{}\\\")" > start_workers.out 2> start_workers.err < /dev/null &
      """.format(scripts_directory, self.node_ip_addresses[0], self.node_ip_addresses[i], num_workers_per_node, remote_worker_path)
      start_workers_commands.append(start_workers_command)
    self._run_command_over_ssh_on_all_nodes_in_parallel(start_workers_commands)

  def stop_ray(self):
    """Kill all of the processes in the Ray cluster.

    This method is used for stopping a Ray cluster. It will ssh to each node and
    kill every schedule, object store, and Python process.
    """
    kill_cluster_command = "killall scheduler objstore python > /dev/null 2> /dev/null"
    self._run_command_over_ssh_on_all_nodes_in_parallel(kill_cluster_command)

  def update_ray(self):
    """Pull the latest Ray source code and rebuild Ray.

    This method is used for updating the Ray source code on a Ray cluster. It
    will ssh to each node, will pull the latest source code from the Ray
    repository, and will rerun the build script (though currently it will not
    rebuild the third party libraries).
    """
    ray_directory = os.path.join(self.installation_directory, "ray")
    update_cluster_command = """
      cd "{}" &&
      git fetch &&
      git reset --hard "@{{upstream}}" -- &&
      (make -C "./build" clean || rm -rf "./build") &&
      ./build.sh
    """.format(ray_directory)
    self._run_command_over_ssh_on_all_nodes_in_parallel(update_cluster_command)

  def _update_worker_code(self, worker_directory):
    """Update the worker code on each node in the cluster.

    This method is used to update the worker source code on each node in the
    cluster. The local worker_directory will be copied under ray_worker_files in
    the installation_directory. For example, if installation_directory is
    "/d/e/f" and we call _update_worker_code("~/a/b/c"), then the contents of
    "~/a/b/c" on the local machine will be copied to "/d/e/f/ray_worker_files/c"
    on each node in the cluster.

    Args:
      worker_directory (str): The path on the local machine to the directory
        that contains the worker code. This directory must contain a file
        worker.py.

    Returns:
      A string with the path to the source code of the worker on the remote
        nodes.
    """
    worker_directory = os.path.expanduser(worker_directory)
    if not os.path.isdir(worker_directory):
      raise Exception("Directory {} does not exist.".format(worker_directory))
    if not os.path.exists(os.path.join(worker_directory, "worker.py")):
      raise Exception("Directory {} does not contain a file named worker.py.".format(worker_directory))
    # If worker_directory is "/a/b/c", then local_directory_name is "c".
    local_directory_name = os.path.split(os.path.realpath(worker_directory))[1]
    remote_directory = os.path.join(self.installation_directory, "ray_worker_files", local_directory_name)
    # Remove and recreate the directory on the node.
    recreate_directory_command = """
      rm -r "{}";
      mkdir -p "{}"
    """.format(remote_directory, remote_directory)
    self._run_command_over_ssh_on_all_nodes_in_parallel(recreate_directory_command)
    # Copy the files from the local machine to the node.
    def copy_function(node_ip_address):
      copy_command = """
        scp -r -i {} {}/* {}@{}:{}/
      """.format(self.key_file, worker_directory, self.username, node_ip_address, remote_directory)
      subprocess.call([copy_command], shell=True)
    inputs = [(node_ip_address,) for node_ip_address in node_ip_addresses]
    self._run_parallel_functions(len(self.node_ip_addresses) * [copy_function], inputs)
    # Return the path to worker.py on the remote nodes.
    remote_worker_path = os.path.join(remote_directory, "worker.py")
    return remote_worker_path

def _is_valid_ip(ip_address):
  """Check if ip_addess is a valid IPv4 address.

  Args:
    ip_address (str): The ip address to check.

  Returns:
    True if the address is a valid IPv4 address and False otherwise.
  """
  try:
    socket.inet_aton(ip_address)
    return True
  except socket.error:
    return False

def _check_ip_addresses(node_ip_addresses):
  """Check if a list of ip addresses are all valid IPv4 addresses.

  This method checks if all of the addresses in a list are valid IPv4 address.
  It prints an error message for each invalid address.

  Args:
    node_ip_addresses (List[str]): The list of ip addresses to check.

  Raises:
    Exception: An exception is raisd if one of the addresses is not a valid IPv4
      address.
  """
  for i, node_ip_address in enumerate(node_ip_addresses):
    if not _is_valid_ip(node_ip_address):
      raise Exception("node_ip_addresses[{}] is '{}', which is not a valid IP address.".format(i, node_ip_address))

if __name__ == "__main__":
  args = parser.parse_args()
  username = args.username
  key_file = args.key_file
  installation_directory = args.installation_directory
  node_ip_addresses = map(lambda s: str(s.strip()), open(args.nodes).readlines())
  cluster = RayCluster(node_ip_addresses, username, key_file, installation_directory)
  IPython.embed()
