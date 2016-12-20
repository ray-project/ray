from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

import ray.services as services

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--node-ip-address", required=True, type=str, help="the ip address of the worker's node")
parser.add_argument("--redis-address", required=False, type=str, help="the address to use for Redis")
parser.add_argument("--num-workers", default=10, required=False, type=int, help="the number of workers to start on this node")
parser.add_argument("--head", action="store_true", help="provide this argument for the head node")

if __name__ == "__main__":
  args = parser.parse_args()
  if args.head:
    if args.redis_address is not None:
      raise Exception("If --head is passed in, a Redis server will be started, so a Redis address should not be provided.")
    address_info = services.start_ray_local(node_ip_address=args.node_ip_address,
                                            num_workers=args.num_workers,
                                            cleanup=False)
  else:
    if args.redis_address is None:
      raise Exception("If --head is not passed in, --redis-address must be provided.")
    address_info = services.start_ray_node(node_ip_address=args.node_ip_address,
                                           redis_address=args.redis_address,
                                           num_workers=args.num_workers,
                                           cleanup=False)
  print(address_info)
