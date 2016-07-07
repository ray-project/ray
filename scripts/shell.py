import os
import argparse
import numpy as np

import ray
import ray.services as services
import ray.worker as worker

import ray.array.remote as ra
import ray.array.distributed as da
import example_functions

DEFAULT_NUM_WORKERS = 10
DEFAULT_WORKER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "default_worker.py")

parser = argparse.ArgumentParser(description="Parse shell options")
parser.add_argument("--scheduler-address", default="127.0.0.1:10001", type=str, help="the scheduler's address")
parser.add_argument("--objstore-address", default="127.0.0.1:20001", type=str, help="the objstore's address")
parser.add_argument("--worker-address", default="127.0.0.1:30001", type=str, help="the worker's address")
parser.add_argument("--attach", action="store_true", help="If true, attach the shell to an already running cluster. If false, start a new cluster.")
parser.add_argument("--worker-path", type=str, help="Path to the worker script")
parser.add_argument("--num-workers", type=int, help="Number of workers to start")

if __name__ == "__main__":
  args = parser.parse_args()
  if args.attach:
    assert args.worker_path is None, "when attaching, no new worker can be started"
    assert args.num_workers is None, "when attaching, no new worker can be started"
    worker.connect(args.scheduler_address, args.objstore_address, args.worker_address, is_driver=True, mode=ray.SHELL_MODE)
  else:
    services.start_ray_local(num_workers=args.num_workers if not args.num_workers is None else DEFAULT_NUM_WORKERS,
                             worker_path=args.worker_path if not args.worker_path is None else DEFAULT_WORKER_PATH,
                             driver_mode=ray.SHELL_MODE)

  import IPython
  IPython.embed()
