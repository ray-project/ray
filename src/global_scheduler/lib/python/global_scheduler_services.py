from __future__ import print_function

import os
import subprocess
import time

def start_global_scheduler(redis_address, use_valgrind=False, use_profiler=False):
  """Start a global scheduler process.

  Args:
    redis_address (str): The address of the Redis instance.
    use_valgrind (bool): True if the global scheduler should be started inside
      of valgrind. If this is True, use_profiler must be False.
    use_profiler (bool): True if the global scheduler should be started inside a
      profiler. If this is True, use_valgrind must be False.

  Return:
    The process ID of the global scheduler process.
  """
  if use_valgrind and use_profiler:
    raise Exception("Cannot use valgrind and profiler at the same time.")
  global_scheduler_executable = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../../build/global_scheduler")
  command = [global_scheduler_executable, "-r", redis_address]
  if use_valgrind:
    pid = subprocess.Popen(["valgrind", "--track-origins=yes", "--leak-check=full", "--show-leak-kinds=all", "--error-exitcode=1"] + command)
    time.sleep(1.0)
  elif use_profiler:
    pid = subprocess.Popen(["valgrind", "--tool=callgrind"] + command)
    time.sleep(1.0)
  else:
    pid = subprocess.Popen(command)
    time.sleep(0.1)
  return pid
