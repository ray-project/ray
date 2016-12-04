from __future__ import print_function

import sys
import argparse
import numpy as np

import ray

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--node-ip-address", required=True, type=str, help="the ip address of the worker's node")
parser.add_argument("--redis-port", required=True, type=int, help="the port to use for Redis")
parser.add_argument("--object-store-name", required=True, type=str, help="the object store's name")
parser.add_argument("--object-store-manager-name", required=True, type=str, help="the object store manager's name")
parser.add_argument("--local-scheduler-name", required=True, type=str, help="the local scheduler's name")

if __name__ == "__main__":
  args = parser.parse_args()
  address_info = {"node_ip_address": args.node_ip_address,
                  "redis_port": args.redis_port,
                  "object_store_names": [args.object_store_name],
                  "object_store_manager_names": [args.object_store_manager_name],
                  "local_scheduler_names": [args.local_scheduler_name]}
  ray.worker.connect(address_info, ray.WORKER_MODE)

  ray.worker.main_loop()
