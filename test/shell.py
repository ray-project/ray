import argparse
import numpy as np

import orchpy
import orchpy.services as services
import orchpy.worker as worker

import test_functions
import arrays.single as single
import arrays.dist as dist

from grpc.beta import implementations
import orchestra_pb2
import types_pb2

TIMEOUT_SECONDS = 5

parser = argparse.ArgumentParser(description='Parse addresses for the worker to connect to.')
parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
parser.add_argument("--worker-address", default="127.0.0.1:30001", type=str, help="the worker's address")

if __name__ == '__main__':
  args = parser.parse_args()
  worker.connect(args.scheduler_address, args.objstore_address, args.worker_address)

  import IPython
  IPython.embed()
