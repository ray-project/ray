from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--node-ip-address", required=True, type=str, help="the ip address of the worker's node")
parser.add_argument("--redis-address", required=True, type=str, help="the address to use for Redis")
parser.add_argument("--object-store-name", required=True, type=str, help="the object store's name")
parser.add_argument("--object-store-manager-name", required=True, type=str, help="the object store manager's name")
parser.add_argument("--local-scheduler-name", required=True, type=str, help="the local scheduler's name")

if __name__ == "__main__":
  args = parser.parse_args()
  info = {"redis_address": args.redis_address,
          "store_socket_name": args.object_store_name,
          "manager_socket_name": args.object_store_manager_name,
          "local_scheduler_socket_name": args.local_scheduler_name}
  ray.worker.connect(info, ray.WORKER_MODE)

  ray.worker.main_loop()
