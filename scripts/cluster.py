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

  def __init__(self, node_ip_addresses, node_private_ip_addresses, username, key_file, installation_directory):
    """Initialize the RayCluster object.

    Args:
      node_ip_addresses (List[str]): A list of the ip addresses of the nodes in
        the cluster. The first element is the head node and will host the
        scheduler process.
      node_private_ip_addresses (List[str]): A list of the ip addresses that the
        nodes use internally to connect to one another. We include this because
        on EC2 communication within a security group must be done over private
        ip addresses.
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
    self.node_private_ip_addresses = node_private_ip_addresses
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

  def run_command_over_ssh_on_all_nodes_in_parallel(self, command):
    """Run a command over ssh on all nodes in the cluster in parallel.

    Args:
      command: This is either a single command to run on every node in the
        cluster over ssh, or it is a list of commands of the same length as
        node_ip_addresses, in which case the ith command will be run on the ith
        element of node_ip_addresses. Currently this command is not allowed to
        have any single quotes.

    Raises:
      Exception: An exception is raised if command is not a string or is not a
        list with the same length as node_ip_addresses.
    """
    if isinstance(command, str):
      # If there is only one command, then run this command on every node in the
      # cluster.
      commands = len(self.node_ip_addresses) * [command]
    else:
      # Otherwise, there is a list of one command for each node in the cluster.
      commands = command
    # Make sure we have one command for each node.
    if len(commands) != len(self.node_ip_addresses):
      raise Exception("The number of commands must match the number of nodes.")
    # Make sure that the commands do not contain any single quotes.
    for command in commands:
      if "'" in command:
        raise Exception("Commands run over ssh must not contain the single quote character. This command does: {}".format(command))
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
    self.run_command_over_ssh_on_all_nodes_in_parallel(install_ray_command)

  def start_ray(self, num_workers_per_node=10):
    """Start Ray on a cluster.

    This method is used to start Ray on a cluster. It will ssh to the head node,
    that is, the first node in the list node_ip_addresses, and it will start the
    scheduler. Then it will ssh to each node and start an object store and some
    workers.

    Args:
      num_workers_per_node (int): The number workers to start on each node.
    """
    scripts_directory = os.path.join(self.installation_directory, "ray/scripts")
    # Start the scheduler
    # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
    start_scheduler_command = """
      cd "{}";
      source ../setup-env.sh;
      python -c "import ray; ray.services.start_scheduler(\\\"{}:10001\\\", cleanup=False)" > start_scheduler.out 2> start_scheduler.err < /dev/null &
    """.format(scripts_directory, self.node_private_ip_addresses[0])
    self._run_command_over_ssh(self.node_ip_addresses[0], start_scheduler_command)

    # Start the workers on each node
    # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
    start_workers_commands = []
    for i, node_ip_address in enumerate(self.node_ip_addresses):
      start_workers_command = """
        cd "{}";
        source ../setup-env.sh;
        python -c "import ray; ray.services.start_node(\\\"{}:10001\\\", \\\"{}\\\", {})" > start_workers.out 2> start_workers.err < /dev/null &
      """.format(scripts_directory, self.node_private_ip_addresses[0], self.node_private_ip_addresses[i], num_workers_per_node)
      start_workers_commands.append(start_workers_command)
    self.run_command_over_ssh_on_all_nodes_in_parallel(start_workers_commands)

    setup_env_path = os.path.join(self.installation_directory, "ray/setup-env.sh")
    print """
      The cluster has been started. You can attach to the cluster by sshing to the head node with the following command.

          ssh -i {} {}@{}

      Then run the following commands.

          source {}  # Add Ray to your Python path.

      Then within a Python interpreter or script, run the following commands.

          import ray
          ray.init(node_ip_address="{}", scheduler_address="{}:10001")
    """.format(self.key_file, self.username, self.node_ip_addresses[0], setup_env_path, self.node_private_ip_addresses[0], self.node_private_ip_addresses[0])

  def stop_ray(self):
    """Kill all of the processes in the Ray cluster.

    This method is used for stopping a Ray cluster. It will ssh to each node and
    kill every schedule, object store, and Python process.
    """
    kill_cluster_command = "killall scheduler objstore python > /dev/null 2> /dev/null"
    self.run_command_over_ssh_on_all_nodes_in_parallel(kill_cluster_command)

  def update_ray(self, branch=None):
    """Pull the latest Ray source code and rebuild Ray.

    This method is used for updating the Ray source code on a Ray cluster. It
    will ssh to each node, will pull the latest source code from the Ray
    repository, and will rerun the build script (though currently it will not
    rebuild the third party libraries).

    Args:
      branch (Optional[str]): The branch to check out. If omitted, then stay on
        the current branch.
    """
    ray_directory = os.path.join(self.installation_directory, "ray")
    change_branch_command = "git checkout -f {}".format(branch) if branch is not None else ""
    update_cluster_command = """
      cd "{}" &&
      git fetch &&
      {}
      git reset --hard "@{{upstream}}" -- &&
      (make -C "./build" clean || rm -rf "./build") &&
      ./build.sh
    """.format(ray_directory, change_branch_command)
    self.run_command_over_ssh_on_all_nodes_in_parallel(update_cluster_command)

  def copy_code_to_cluster(self, user_source_directory):
    """Update the user's source code on each node in the cluster.

    This method is used to copy the user's source code on each node in the
    cluster. The local user_source_directory will be copied under
    ray_source_files in the home directory on the worker node. For example, if
    we call copy_code_to_cluster("~/a/b/c"), then the contents of "~/a/b/c" on
    the local machine will be copied to "~/ray_source_files/c" on each node in
    the cluster.

    Args:
      user_source_directory (str): The path on the local machine to the directory
        that contains the worker code.

    Returns:
      A string with the path to the source code of the worker on the remote
        nodes.
    """
    user_source_directory = os.path.expanduser(user_source_directory)
    if not os.path.isdir(user_source_directory):
      raise Exception("Directory {} does not exist.".format(user_source_directory))
    # If user_source_directory is "/a/b/c", then local_directory_name is "c".
    local_directory_name = os.path.split(os.path.realpath(user_source_directory))[1]
    remote_directory = os.path.join(self.installation_directory, "ray_source_files", local_directory_name)
    # Remove and recreate the directory on the node.
    recreate_directory_command = """
      rm -r "{}";
      mkdir -p "{}"
    """.format(remote_directory, remote_directory)
    self.run_command_over_ssh_on_all_nodes_in_parallel(recreate_directory_command)
    # Copy the files from the local machine to the node.
    def copy_function(node_ip_address):
      copy_command = """
        scp -r -i {} {}/* {}@{}:{}/
      """.format(self.key_file, user_source_directory, self.username, node_ip_address, remote_directory)
      subprocess.call([copy_command], shell=True)
    inputs = [(node_ip_address,) for node_ip_address in node_ip_addresses]
    self._run_parallel_functions(len(self.node_ip_addresses) * [copy_function], inputs)
    # Return the source directory path on the remote nodes
    return remote_directory

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
  node_ip_addresses = []
  node_private_ip_addresses = []
  # Check if the IP addresses in the nodes file are valid.
  for line in open(args.nodes).readlines():
    parts = line.split(",")
    ip_address = str(parts[0].strip())
    if len(parts) == 1:
      private_ip_address = ip_address
    elif len(parts) == 2:
      private_ip_address = str(parts[1].strip())
    else:
      raise Exception("Each line in the nodes file must have either one or two ip addresses.")
    node_ip_addresses.append(ip_address)
    node_private_ip_addresses.append(private_ip_address)
  # This command finds the home directory on the cluster. That directory will be
  # used for installing Ray. Note that single quotes around 'echo $HOME' are
  # important. If you use double quotes, then the $HOME environment variable
  # will be expanded locally instead of remotely.
  echo_home_command = "ssh -o StrictHostKeyChecking=no -i {} {}@{} 'echo $HOME'".format(key_file, username, node_ip_addresses[0])
  installation_directory = subprocess.check_output(echo_home_command, shell=True).strip()
  print "Using '{}' as the home directory on the cluster.".format(installation_directory)
  # Create the Raycluster object.
  cluster = RayCluster(node_ip_addresses, node_private_ip_addresses, username, key_file, installation_directory)
  # Drop into an IPython shell.
  IPython.embed()
