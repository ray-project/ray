#!/usr/bin/env python

import os
import sys
import numpy as np

import ray

def main(argv):
  DEFAULT_NUM_WORKERS = 1
  DEFAULT_WORKER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "default_worker.py")

  import argparse  # No need for this to be global
  parser = argparse.ArgumentParser(description="Parse shell options")
  parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
  parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
  parser.add_argument("--worker-address", default="127.0.0.1:30001", type=str, help="the worker's address")
  parser.add_argument("--attach", action="store_true", help="If true, attach the shell to an already running cluster. If false, start a new cluster.")
  parser.add_argument("--worker-path", type=str, help="Path to the worker script")
  parser.add_argument("--num-workers", type=int, help="Number of workers to start")

  args, unknown_args = parser.parse_known_args(argv)
  if args.attach:
    assert args.worker_path is None, "when attaching, no new worker can be started"
    assert args.num_workers is None, "when attaching, no new worker can be started"
    ray.worker.connect(args.scheduler_address, args.objstore_address, args.worker_address, is_driver=True, mode=ray.SHELL_MODE)
  else:
    ray.services.start_ray_local(num_workers=args.num_workers if not args.num_workers is None else DEFAULT_NUM_WORKERS,
                                 worker_path=args.worker_path if not args.worker_path is None else DEFAULT_WORKER_PATH,
                                 driver_mode=ray.SHELL_MODE)
  return unknown_args

if __name__ == "__main__":
  import IPython
  IPython.terminal.ipapp.launch_new_instance(argv=main(sys.argv[1:]), user_ns=globals())
