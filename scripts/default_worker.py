import sys
import argparse
import numpy as np

import ray

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--user-source-directory", type=str, help="the directory containing the user's application code")
parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
parser.add_argument("--worker-address", default="127.0.0.1:40001", type=str, help="the worker's address")

if __name__ == "__main__":
  args = parser.parse_args()
  if args.user_source_directory is not None:
    # Adding the directory containing the user's application code to the Python
    # path so that the worker can import Python modules from this directory. We
    # insert into the first position (as opposed to the zeroth) because the
    # zeroth position is reserved for the empty string.
    sys.path.insert(1, args.user_source_directory)
  ray.worker.connect(args.scheduler_address, args.objstore_address, args.worker_address)

  ray.worker.main_loop()
