import sys
import argparse
import numpy as np

import test_functions
import arrays.single as single
import arrays.dist as dist

import halo
import halo.services as services
import halo.worker as worker

parser = argparse.ArgumentParser(description='Parse addresses for the worker to connect to.')
parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
parser.add_argument("--worker-address", default="127.0.0.1:40001", type=str, help="the worker's address")

if __name__ == '__main__':
  args = parser.parse_args()
  worker.connect(args.scheduler_address, args.objstore_address, args.worker_address)

  halo.register_module(test_functions)
  halo.register_module(single)
  halo.register_module(single.random)
  halo.register_module(single.linalg)
  halo.register_module(dist)
  halo.register_module(dist.random)
  halo.register_module(dist.linalg)
  halo.register_module(sys.modules[__name__])

  worker.main_loop()
