# This script can be used to start Ray on an existing cluster.
#
# How to use it: Create a file "nodes.txt" that contains a list of the IP
# addresses of the nodes in the cluster. Put the head node first. This node will
# host the driver and the scheduler.

import os
import subprocess
import socket
import argparse
import threading
import IPython

parser = argparse.ArgumentParser(description="Parse information about the cluster.")
parser.add_argument("--nodes", type=str, required=True, help="Test file with node IP addresses, one line per address.")
parser.add_argument("--key-file", type=str, required=True, help="Path to the file that contains the private key.")
parser.add_argument("--username", type=str, required=True, help="User name for logging in.")
parser.add_argument("--installation-directory", type=str, required=True, help="The directory in which to install Ray.")

def run_command_over_ssh(node_ip_address, username, key_file, command):
  """
  This method is used for connecting to a node with ssh and running a sequence
    of commands.

  :param node_ip_address: the ip address of the node to ssh to
  :param username: the username used to ssh to the cluster
  :param key_file: the key used to ssh to the cluster
  :param command: the command to run over ssh, currently this command is not allowed to have any single quotes
  """
  if "'" in command:
    raise Exception("Commands run over ssh must not contain the single quote character. This command does: {}".format(command))
  full_command = "ssh -o StrictHostKeyChecking=no -i {} {}@{} '{}'".format(key_file, username, node_ip_address, command)
  subprocess.call([full_command], shell=True)
  print "Finished running command '{}' on {}@{}.".format(command, username, node_ip_address)

def _install_ray(node_ip_addresses, username, key_file, installation_directory):
  """
  This method is used to install Ray on a cluster. For each node in the cluster,
    it will ssh to the node and run the build scripts.

  :param node_ip_addresses: ip addresses of the nodes on which to install Ray
  :param username: the username used to ssh to the cluster
  :param key_file: the key used to ssh to the cluster
  :param installation_directory: directory in which Ray is installed, for example "/home/ubuntu/"
  """
  def install_ray_over_ssh(node_ip_address, username, key_file, installation_directory):
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
    """.format(installation_directory, installation_directory)
    run_command_over_ssh(node_ip_address, username, key_file, install_ray_command)
  threads = []
  for node_ip_address in node_ip_addresses:
    t = threading.Thread(target=install_ray_over_ssh, args=(node_ip_address, username, key_file, installation_directory))
    t.start()
    threads.append(t)
  for t in threads:
    t.join()

def _start_ray(node_ip_addresses, username, key_file, num_workers_per_node, worker_directory, installation_directory):
  """
  This method is used to start Ray on a cluster. It will ssh to the head node,
    that is, the first node in the list node_ip_addresses, and it will start
    the scheduler. Then it will ssh to each node and start an object store and
    some workers.

  :param node_ip_addresses: ip addresses of the nodes on which to install Ray
  :param username: the username used to ssh to the cluster
  :param key_file: the key used to ssh to the cluster
  :param worker_directory: local directory containing the worker source code
  :param installation_directory: directory in which Ray is installed, for example "/home/ubuntu/"
  """
  # First update the worker code on the nodes.
  remote_worker_path = _update_worker_code(node_ip_addresses, worker_directory, installation_directory)

  scripts_directory = os.path.join(installation_directory, "ray/scripts")
  # Start the scheduler
  # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
  start_scheduler_command = """
    cd "{}";
    source ../setup-env.sh;
    python -c "import ray; ray.services.start_scheduler(\\\"{}:10001\\\", local=False)" > start_scheduler.out 2> start_scheduler.err < /dev/null &
  """.format(scripts_directory, node_ip_addresses[0])
  run_command_over_ssh(node_ip_addresses[0], username, key_file, start_scheduler_command)

  # Start the workers on each node
  # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
  for i, node_ip_address in enumerate(node_ip_addresses):
    start_workers_command = """
      cd "{}";
      source ../setup-env.sh;
      python -c "import ray; ray.services.start_node(\\\"{}:10001\\\", \\\"{}\\\", {}, worker_path=\\\"{}\\\")" > start_workers.out 2> start_workers.err < /dev/null &
    """.format(scripts_directory, node_ip_addresses[0], node_ip_addresses[i], num_workers_per_node, remote_worker_path)
    run_command_over_ssh(node_ip_address, username, key_file, start_workers_command)

  print "cluster started; you can start the shell on the head node with:"
  setup_env_path = os.path.join(args.installation_directory, "ray/setup-env.sh")
  shell_script_path = os.path.join(args.installation_directory, "ray/scripts/shell.py")
  print """
    source "{}";
    python "{}" --scheduler-address={}:10001 --objstore-address={}:20001 --worker-address={}:30001 --attach
  """.format(setup_env_path, shell_script_path, node_ip_addresses[0], node_ip_addresses[0], node_ip_addresses[0])

def _restart_workers(node_ip_addresses, username, key_file, num_workers_per_node, worker_directory, installation_directory):
  """
  This method is used for restarting the workers in the cluster, for example, to
    use new application code. This is done without shutting down the scheduler
    or the object stores so that work is not thrown away. It also does not shut
    down any drivers.

  :param node_ip_addresses: ip addresses of the nodes on which to restart the workers
  :param username: the username used to ssh to the cluster
  :param key_file: the key used to ssh to the cluster
  :param worker_directory: local directory containing the worker source code
  :param installation_directory: directory in which Ray is installed, for example "/home/ubuntu/"
  """
  # First update the worker code on the nodes.
  remote_worker_path = _update_worker_code(node_ip_addresses, worker_directory, installation_directory)

  scripts_directory = os.path.join(installation_directory, "ray/scripts")
  head_node_ip_address = node_ip_addresses[0]
  scheduler_address = "{}:10001".format(head_node_ip_address) # This needs to be the address of the currently running scheduler, which was presumably created in _start_ray.
  objstore_address = "{}:20001".format(head_node_ip_address) # This needs to be the address of the currently running object store, which was presumably created in _start_ray.
  shell_address = "{}:30000".format(head_node_ip_address) # This address must be currently unused. In particular, it cannot be the address of any currently running shell.

  # Kill the current workers by attaching a driver to the scheduler and calling ray.kill_workers()
  # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
  kill_workers_command = """
    cd "{}";
    source ../setup-env.sh;
    python -c "import ray; ray.connect(\\\"{}\\\", \\\"{}\\\", \\\"{}\\\", is_driver=True); ray.kill_workers()"
  """.format(scripts_directory, scheduler_address, objstore_address, shell_address)
  run_command_over_ssh(head_node_ip_address, username, key_file, kill_workers_command)

  # Start new workers on each node
  # The triple backslashes are used for two rounds of escaping, something like \\\" -> \" -> "
  for i, node_ip_address in enumerate(node_ip_addresses):
    start_workers_command = """
      cd "{}";
      source ../setup-env.sh;
      python -c "import ray; ray.services.start_workers(\\\"{}:10001\\\", \\\"{}:20001\\\", {}, worker_path=\\\"{}\\\")" > start_workers.out 2> start_workers.err < /dev/null &
    """.format(scripts_directory, node_ip_addresses[0], node_ip_addresses[i], num_workers_per_node, remote_worker_path)
    run_command_over_ssh(node_ip_address, username, key_file, start_workers_command)

def _stop_ray(node_ip_addresses, username, key_file):
  """
  This method is used for stopping a Ray cluster. It will ssh to each node and
    kill every schedule, object store, and Python process.

  :param node_ip_addresses: ip addresses of the nodes on which to restart the workers
  :param username: the username used to ssh to the cluster
  :param key_file: the key used to ssh to the cluster
  """
  kill_cluster_command = "killall scheduler objstore python > /dev/null 2> /dev/null"
  for node_ip_address in node_ip_addresses:
    run_command_over_ssh(node_ip_address, username, key_file, kill_cluster_command)

def _update_ray(node_ip_addresses, username, key_file, installation_directory):
  """
  This method is used for updating the Ray source code on a Ray cluster. It
    will ssh to each node, will pull the latest source code from the Ray
    repository, and will rerun the build script (though currently it will not
    rebuild the third party libraries).

  :param node_ip_addresses: ip addresses of the nodes on which to restart the workers
  :param username: the username used to ssh to the cluster
  :param key_file: the key used to ssh to the cluster
  :param installation_directory: directory in which Ray is installed, for example "/home/ubuntu/"
  """
  ray_directory = os.path.join(installation_directory, "ray")
  update_cluster_command = """
    cd "{}" &&
    git fetch &&
    git reset --hard "@{{upstream}}" -- &&
    (make -C "./build" clean || rm -rf "./build") &&
    ./build.sh
  """.format(ray_directory)
  for node_ip_address in node_ip_addresses:
    run_command_over_ssh(node_ip_address, username, key_file, update_cluster_command)

def _update_worker_code(node_ip_addresses, worker_directory, installation_directory):
  """
  This method is used to sync update the worker source code on each node in the
    cluster. The worker_directory will be copied under installation_directory.
    For example, we call _update_worker_code(node_ip_addresses, "~/a/b/c",
    "/d/e/f"), then the contents of ~/a/b/c on the local machine will be copied
    to /d/e/f/ray_worker_files/c on each node in the cluster.

  :param node_ip_addresses: ip addresses of the nodes on which to restart the
    workers
  :param worker_directory: The path on the local machine to the directory that
    contains the worker code. This directory must contain a file worker.py.
  :param installation_directory: Directory in which ray is installed, for
    example "/home/ubuntu/".

  :rtype: A string with the path to the source code of the worker on the remote
    nodes.
  """
  worker_directory = os.path.expanduser(worker_directory)
  if not os.path.isdir(worker_directory):
    raise Exception("Directory {} does not exist.".format(worker_directory))
  if not os.path.exists(os.path.join(worker_directory, "worker.py")):
    raise Exception("Directory {} does not contain a file named worker.py.".format(worker_directory))
  # If worker_directory is "/a/b/c", then local_directory_name is "c".
  local_directory_name = os.path.split(os.path.realpath(worker_directory))[1]
  remote_directory = os.path.join(installation_directory, "ray_worker_files", local_directory_name)
  for node_ip_address in node_ip_addresses:
    # Remove and recreate the directory on the node.
    recreate_directory_command = """
      rm -r "{}";
      mkdir -p "{}"
    """.format(remote_directory, remote_directory)
    run_command_over_ssh(node_ip_address, username, key_file, recreate_directory_command)
    # Copy the files from the local machine to the node.
    copy_command = """
      scp -r -i {} {}/* {}@{}:{}/
    """.format(key_file, worker_directory, username, node_ip_address, remote_directory)
    subprocess.call([copy_command], shell=True)
  remote_worker_path = os.path.join(remote_directory, "worker.py")
  return remote_worker_path

def is_valid_ip(ip_address):
  """
  This method returns true if an address is a valid IPv4 address and returns
    false otherwise.

  :param ip_address: the ip address to check
  """
  try:
    socket.inet_aton(ip_address)
    return True
  except socket.error:
    return False

def check_ip_addresses(node_ip_addresses):
  """
  This method checks if all of the addresses in a list are valid IPv4 address.
    If not, it returns false and prints an error message for each invalid
    address.

  :param node_ip_addresses: the list of ip addresses to check
  """
  addresses_valid = True
  for index, node_ip_address in enumerate(node_ip_addresses):
    if not is_valid_ip(node_ip_address):
      print "ERROR: node_ip_addresses[{}] is '{}', which is not a valid IP address.".format(index, node_ip_address)
      addresses_valid = False
  return addresses_valid

if __name__ == "__main__":
  args = parser.parse_args()
  username = args.username
  key_file = args.key_file
  installation_directory = args.installation_directory
  node_ip_addresses = map(lambda s: str(s.strip()), open(args.nodes).readlines())

  def install_ray(node_ip_addresses=node_ip_addresses):
    """
    This method is used to install Ray on a cluster. For each node in the cluster,
      it will ssh to the node and run the build scripts.

    :param node_ip_addresses: ip addresses of the nodes on which to install Ray
    """
    if check_ip_addresses(node_ip_addresses):
      _install_ray(node_ip_addresses, username, key_file, installation_directory)

  def start_ray(worker_directory, num_workers_per_node=10, node_ip_addresses=node_ip_addresses):
    """
    This method is used to start Ray on a cluster. It will ssh to the head node,
      that is, the first node in the list node_ip_addresses, and it will start
      the scheduler. Then it will ssh to each node and start an object store and
      some workers.

    :param worker_directory: path of the source code to have the workers run
    :param node_ip_addresses: ip addresses of the nodes on which to install Ray
    """
    if check_ip_addresses(node_ip_addresses):
      _start_ray(node_ip_addresses, username, key_file, num_workers_per_node, worker_directory, installation_directory)

  def restart_workers(worker_directory, num_workers_per_node=10, node_ip_addresses=node_ip_addresses):
    """
    This method is used for restarting the workers in the cluster, for example, to
      use new application code. This is done without shutting down the scheduler
      or the object stores so that work is not thrown away. It also does not
      shut down any drivers.

    :param node_ip_addresses: ip addresses of the nodes on which to restart the workers
    :param worker_directory: path of the source code to have the workers run
    :param installation_directory: directory in which Ray is installed, for example "/home/ubuntu/"
    """
    if check_ip_addresses(node_ip_addresses):
      _restart_workers(node_ip_addresses, username, key_file, num_workers_per_node, worker_directory, installation_directory)

  def stop_ray(node_ip_addresses=node_ip_addresses):
    """
    This method is used for stopping a Ray cluster. It will ssh to each node and
      kill every schedule, object store, and Python process.

    :param node_ip_addresses: ip addresses of the nodes on which to restart the workers
    """
    if check_ip_addresses(node_ip_addresses):
      _stop_ray(node_ip_addresses, username, key_file)

  def update_ray(node_ip_addresses=node_ip_addresses):
    """
    This method is used for updating the Ray source code on a Ray cluster. It
      will ssh to each node, will pull the latest source code from the Ray
      repository, and will rerun the build script (though currently it will not
      rebuild the third party libraries).

    :param node_ip_addresses: ip addresses of the nodes on which to restart the workers
    """
    if check_ip_addresses(node_ip_addresses):
      _update_ray(node_ip_addresses, username, key_file, installation_directory)

  IPython.embed()
