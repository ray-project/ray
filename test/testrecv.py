import sys
import argparse
import numpy as np

import arrays.single as single
import arrays.dist as dist

import orchpy
import orchpy.services as services
import orchpy.worker as worker

parser = argparse.ArgumentParser(description='Parse addresses for the worker to connect to.')
parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
parser.add_argument("--worker-address", default="127.0.0.1:40001", type=str, help="the worker's address")

@orchpy.distributed([], [np.ndarray])
def test_alias_f():
  return np.ones([3, 4, 5])

@orchpy.distributed([], [np.ndarray])
def test_alias_g():
  return f()


@orchpy.distributed([str], [str])
def print_string(string):
  print "called print_string with", string
  f = open("asdfasdf.txt", "w")
  f.write("successfully called print_string with argument {}.".format(string))
  return string

@orchpy.distributed([int, int], [int, int])
def handle_int(a, b):
  return a + 1, b + 1

if __name__ == '__main__':
  args = parser.parse_args()
  worker.connect(args.scheduler_address, args.objstore_address, args.worker_address)

  orchpy.register_module(single)
  orchpy.register_module(single.random)
  orchpy.register_module(single.linalg)
  orchpy.register_module(dist)
  orchpy.register_module(dist.random)
  # orchpy.register_module(dist.linalg)
  orchpy.register_module(sys.modules[__name__])

  worker.main_loop()
