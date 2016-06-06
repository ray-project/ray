import sys
import argparse
import numpy as np

import test_functions
import halo.arrays.remote as ra
import halo.arrays.distributed as da

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
  halo.register_module(ra)
  halo.register_module(ra.random)
  halo.register_module(ra.linalg)
  halo.register_module(da)
  halo.register_module(da.random)
  halo.register_module(da.linalg)
  halo.register_module(sys.modules[__name__])

  worker.main_loop()
