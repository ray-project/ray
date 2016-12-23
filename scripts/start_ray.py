from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import redis
import socket

import ray.services as services

parser = argparse.ArgumentParser(description="Parse addresses for the worker to connect to.")
parser.add_argument("--node-ip-address", required=False, type=str, help="the IP address of the worker's node")
parser.add_argument("--redis-address", required=False, type=str, help="the address to use for Redis")
parser.add_argument("--num-workers", default=10, required=False, type=int, help="the number of workers to start on this node")
parser.add_argument("--head", action="store_true", help="provide this argument for the head node")

def check_no_existing_redis_clients(node_ip_address, redis_address):
  redis_host, redis_port = redis_address.split(":")
  redis_client = redis.StrictRedis(host=redis_host, port=int(redis_port))
  # The client table prefix must be kept in sync with the file
  # "src/common/redis_module/ray_redis_module.c" where it is defined.
  REDIS_CLIENT_TABLE_PREFIX = "CL:"
  client_keys = redis_client.keys("{}*".format(REDIS_CLIENT_TABLE_PREFIX))
  # Filter to clients on the same node and do some basic checking.
  for key in client_keys:
    info = redis_client.hgetall(key)
    assert b"ray_client_id" in info
    assert b"node_ip_address" in info
    assert b"client_type" in info
    if info[b"node_ip_address"].decode("ascii") == node_ip_address:
      raise Exception("This Redis instance is already connected to clients with this IP address.")

def get_head_node_ip_address():
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.connect(("8.8.8.8", 53))
  return s.getsockname()[0]

def get_node_ip_address(redis_address):
  redis_host, redis_port = redis_address.split(":")
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.connect((redis_host, int(redis_port)))
  return s.getsockname()[0]

if __name__ == "__main__":
  args = parser.parse_args()

  # Note that we redirect stdout and stderr to /dev/null because otherwise
  # attempts to print may cause exceptions if a process is started inside of an
  # SSH connection and the SSH connection dies. TODO(rkn): This is a temporary
  # fix. We should actually redirect stdout and stderr to Redis in some way.

  if args.head:
    # Start Ray on the head node.
    if args.redis_address is not None:
      raise Exception("If --head is passed in, a Redis server will be started, so a Redis address should not be provided.")
    if args.node_ip_address is None:
      node_ip_address = get_head_node_ip_address()
    else:
      node_ip_address = args.node_ip_address
    print("Using IP address {} for this node.".format(node_ip_address))
    address_info = services.start_ray_local(node_ip_address=node_ip_address,
                                            num_workers=args.num_workers,
                                            cleanup=False,
                                            redirect_output=True)
  else:
    # Start Ray on a non-head node.
    if args.redis_address is None:
      raise Exception("If --head is not passed in, --redis-address must be provided.")
    redis_host, redis_port = args.redis_address.split(":")
    # Wait for the Redis server to be started. And throw an exception if we
    # can't connect to it.
    services.wait_for_redis_to_start(redis_host, int(redis_port))
    # Get the node IP address if one is not provided by connecting with Redis.
    if args.node_ip_address is None:
      node_ip_address = get_node_ip_address(args.redis_address)
    else:
      node_ip_addess = args.node_ip_address
    print("Using IP address {} for this node.".format(node_ip_address))
    # Check that there aren't already Redis clients with the same IP address
    # connected with this Redis instance. This raises an exception if the Redis
    # server already has clients on this node.
    check_no_existing_redis_clients(node_ip_address, args.redis_address)
    address_info = services.start_ray_node(node_ip_address=node_ip_address,
                                           redis_address=args.redis_address,
                                           num_workers=args.num_workers,
                                           cleanup=False,
                                           redirect_output=True)
  print(address_info)
