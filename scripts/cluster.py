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
  full_command = "ssh -i {} {}@{} '{}'".format(key_file, username, node_ip_address, command)
  subprocess.call([full_command], shell=True)
  print "Finished running command '{}' on {}@{}.".format(command, username, node_ip_address)

def install_ray_multi_node(node_ip_addresses, username, key_file, installation_directory):
  def install_ray_over_ssh(node_ip_address, username, key_file, installation_directory):
    install_ray_command = "sudo apt-get update; sudo apt-get -y install git; mkdir -p {}; cd {}; git clone https://github.com/amplab/ray; cd ray; ./setup.sh".format(installation_directory, installation_directory)
    run_command_over_ssh(node_ip_address, username, key_file, install_ray_command)
  threads = []
  for node_ip_address in node_ip_addresses:
    t = threading.Thread(target=install_ray_over_ssh, args=(node_ip_address, username, key_file, installation_directory))
    t.start()
    threads.append(t)
  for t in threads:
    t.join()

def start_ray_multi_node(node_ip_addresses, username, key_file, worker_path, installation_directory):
  build_directory = os.path.join(installation_directory, "ray/build")
  start_scheduler_command = "cd {}; nohup ./scheduler {}:10001 > scheduler.out 2> scheduler.err < /dev/null &".format(build_directory, node_ip_addresses[0])
  run_command_over_ssh(node_ip_addresses[0], username, key_file, start_scheduler_command)

  for i, node_ip_address in enumerate(node_ip_addresses):
    scripts_directory = os.path.join(installation_directory, "ray/scripts")
    start_workers_command = "cd {}; python start_workers.py --scheduler-address={}:10001 --node-ip={} --worker-path={} > start_workers.out 2> start_workers.err < /dev/null &".format(scripts_directory, node_ip_addresses[0], node_ip_addresses[i], worker_path)
    run_command_over_ssh(node_ip_address, username, key_file, start_workers_command)

  print "cluster started; you can start the shell on the head node with:"
  shell_script_path = os.path.join(args.installation_directory, "ray/scripts/shell.py")
  print "python {} --scheduler-address={}:10001 --objstore-address={}:20001 --worker-address={}:30001".format(shell_script_path, node_ip_addresses[0], node_ip_addresses[0], node_ip_addresses[0])

def stop_ray_multi_node(node_ip_addresses, username, key):
  for node_ip_address in node_ip_addresses:
    kill_cluster_command = "killall scheduler objstore python > /dev/null 2> /dev/null"
    run_command_over_ssh(node_ip_address, username, key_file, kill_cluster_command)

# Returns true if address is a valid IPv4 address and false otherwise.
def is_valid_ip(ip_address):
  try:
    socket.inet_aton(ip_address)
    return True
  except socket.error:
    return False

if __name__ == "__main__":
  args = parser.parse_args()
  username = args.username
  key_file = args.key_file
  installation_directory = args.installation_directory
  node_ip_addresses = map(lambda s: str(s.strip()), open(args.nodes).readlines())
  for index, node_ip_address in enumerate(node_ip_addresses):
    if not is_valid_ip(node_ip_address):
      print "\nWARNING: The string '{}' from line {} in the file {} is not a valid IP address.\n".format(node_ip_address, index + 1, args.nodes)

  def install_ray(node_ip_addresses):
    install_ray_multi_node(node_ip_addresses, username, key_file, installation_directory)

  def start_ray(node_ip_addresses, worker_path):
    start_ray_multi_node(node_ip_addresses, username, key_file, worker_path, installation_directory)

  def stop_ray(node_ip_addresses):
    stop_ray_multi_node(node_ip_addresses, username, key_file)

  IPython.embed()
