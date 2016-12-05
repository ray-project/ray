from __future__ import print_function

import os
import random
import subprocess
import time

def random_name():
  return str(random.randint(0, 99999999))

def start_local_scheduler(plasma_store_name, plasma_manager_name=None, redis_address=None, use_valgrind=False, use_profiler=False):
  """Start a local scheduler process.

  Args:
    plasma_store_name (str): The name of the plasma store socket to connect to.
    plasma_manager_name (str): The name of the plasma manager to connect to.
      This does not need to be provided, but if it is, then the Redis address
      must be provided as well.
    redis_address (str): The address of the Redis instance to connect to. If
      this is not provided, then the local scheduler will not connect to Redis.
    use_valgrind (bool): True if the local scheduler should be started inside of
      valgrind. If this is True, use_profiler must be False.
    use_profiler (bool): True if the local scheduler should be started inside a
      profiler. If this is True, use_valgrind must be False.

  Return:
    A tuple of the name of the local scheduler socket and the process ID of the
      local scheduler process.
  """
  if (plasma_manager_name == None) != (redis_address == None):
    raise Exception("If one of the plasma_manager_name and the redis_address is provided, then both must be provided.")
  if use_valgrind and use_profiler:
    raise Exception("Cannot use valgrind and profiler at the same time.")
  local_scheduler_executable = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../build/photon_scheduler")
  local_scheduler_name = "/tmp/scheduler{}".format(random_name())
  command = [local_scheduler_executable, "-s", local_scheduler_name, "-p", plasma_store_name]
  if plasma_manager_name is not None:
    command += ["-m", plasma_manager_name]
  if redis_address is not None:
    command += ["-r", redis_address]
  if use_valgrind:
    pid = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + command)
    time.sleep(1.0)
  elif use_profiler:
    pid = subprocess.Popen(["valgrind", "--tool=callgrind"] + command)
    time.sleep(1.0)
  else:
    pid = subprocess.Popen(command)
    time.sleep(0.1)
  return local_scheduler_name, pid
