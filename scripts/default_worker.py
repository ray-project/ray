import sys
import argparse
import numpy as np

import ray

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--user-source-directory", type=str, help="the directory containing the user's application code")
parser.add_argument("--node-ip-address", required=True, type=str, help="the ip address of the worker's node")
parser.add_argument("--scheduler-address", required=True, type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", type=str, help="the objstore's address")

if __name__ == "__main__":
  args = parser.parse_args()
  if args.user_source_directory is not None:
    # Adding the directory containing the user's application code to the Python
    # path so that the worker can import Python modules from this directory. We
    # insert into the first position (as opposed to the zeroth) because the
    # zeroth position is reserved for the empty string.
    sys.path.insert(1, args.user_source_directory)
  ray.worker.connect(args.node_ip_address, args.scheduler_address)

  ray.worker.main_loop()
