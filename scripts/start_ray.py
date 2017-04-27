from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import redis

import ray.services as services

parser = argparse.ArgumentParser(
    description="Start the Ray processes on a node.")
parser.add_argument("--node-ip-address", required=False, type=str,
                    help="the IP address of this node")
parser.add_argument("--redis-address", required=False, type=str,
                    help="the address to use for connecting to Redis")
parser.add_argument("--redis-port", required=False, type=str,
                    help="the port to use for starting Redis")
parser.add_argument("--num-redis-shards", required=False, type=int,
                    help=("the number of additional Redis shards to use in "
                          "addition to the primary Redis shard"))
parser.add_argument("--object-manager-port", required=False, type=int,
                    help="the port to use for starting the object manager")
parser.add_argument("--num-workers", required=False, type=int,
                    help="the initial number of workers to start on this node")
parser.add_argument("--num-cpus", required=False, type=int,
                    help="the number of CPUs on this node")
parser.add_argument("--num-gpus", required=False, type=int,
                    help="the number of GPUs on this node")
parser.add_argument("--head", action="store_true",
                    help="provide this argument for the head node")


def check_no_existing_redis_clients(node_ip_address, redis_address):
  redis_ip_address, redis_port = redis_address.split(":")
  redis_client = redis.StrictRedis(host=redis_ip_address, port=int(redis_port))
  # The client table prefix must be kept in sync with the file
  # "src/common/redis_module/ray_redis_module.cc" where it is defined.
  REDIS_CLIENT_TABLE_PREFIX = "CL:"
  client_keys = redis_client.keys("{}*".format(REDIS_CLIENT_TABLE_PREFIX))
  # Filter to clients on the same node and do some basic checking.
  for key in client_keys:
    info = redis_client.hgetall(key)
    assert b"ray_client_id" in info
    assert b"node_ip_address" in info
    assert b"client_type" in info
    assert b"deleted" in info
    # Clients that ran on the same node but that are marked dead can be
    # ignored.
    deleted = info[b"deleted"]
    deleted = bool(int(deleted))
    if deleted:
      continue

    if info[b"node_ip_address"].decode("ascii") == node_ip_address:
      raise Exception("This Redis instance is already connected to clients "
                      "with this IP address.")


if __name__ == "__main__":
  args = parser.parse_args()

  # Note that we redirect stdout and stderr to /dev/null because otherwise
  # attempts to print may cause exceptions if a process is started inside of an
  # SSH connection and the SSH connection dies. TODO(rkn): This is a temporary
  # fix. We should actually redirect stdout and stderr to Redis in some way.

  if args.head:
    # Start Ray on the head node.
    if args.redis_address is not None:
      raise Exception("If --head is passed in, a Redis server will be "
                      "started, so a Redis address should not be provided.")

    # Get the node IP address if one is not provided.
    if args.node_ip_address is None:
      node_ip_address = services.get_node_ip_address()
    else:
      node_ip_address = args.node_ip_address
    print("Using IP address {} for this node.".format(node_ip_address))

    address_info = {}
    # Use the provided Redis port if there is one.
    if args.redis_port is not None:
      address_info["redis_address"] = "{}:{}".format(node_ip_address,
                                                     args.redis_port)
    # Use the provided object manager port if there is one.
    if args.object_manager_port is not None:
      address_info["object_manager_ports"] = [args.object_manager_port]
    if address_info == {}:
      address_info = None

    address_info = services.start_ray_head(
      address_info=address_info,
      node_ip_address=node_ip_address,
      num_workers=args.num_workers,
      cleanup=False,
      redirect_output=True,
      num_cpus=args.num_cpus,
      num_gpus=args.num_gpus,
      num_redis_shards=args.num_redis_shards)
    print(address_info)
    print("\nStarted Ray on this node. You can add additional nodes to the "
          "cluster by calling\n\n"
          "    ./scripts/start_ray.sh --redis-address {}\n\n"
          "from the node you wish to add. You can connect a driver to the "
          "cluster from Python by running\n\n"
          "    import ray\n"
          "    ray.init(redis_address=\"{}\")\n\n"
          "If you have trouble connecting from a different machine, check "
          "that your firewall is configured properly. If you wish to "
          "terminate the processes that have been started, run\n\n"
          "    ./scripts/stop_ray.sh".format(address_info["redis_address"],
                                             address_info["redis_address"]))
  else:
    # Start Ray on a non-head node.
    if args.redis_port is not None:
      raise Exception("If --head is not passed in, --redis-port is not "
                      "allowed")
    if args.redis_address is None:
      raise Exception("If --head is not passed in, --redis-address must be "
                      "provided.")
    if args.num_redis_shards is not None:
      raise Exception("If --head is not passed in, --num-redis-shards must "
                      "not be provided.")
    redis_ip_address, redis_port = args.redis_address.split(":")
    # Wait for the Redis server to be started. And throw an exception if we
    # can't connect to it.
    services.wait_for_redis_to_start(redis_ip_address, int(redis_port))
    # Get the node IP address if one is not provided.
    if args.node_ip_address is None:
      node_ip_address = services.get_node_ip_address(args.redis_address)
    else:
      node_ip_address = args.node_ip_address
    print("Using IP address {} for this node.".format(node_ip_address))
    # Check that there aren't already Redis clients with the same IP address
    # connected with this Redis instance. This raises an exception if the Redis
    # server already has clients on this node.
    check_no_existing_redis_clients(node_ip_address, args.redis_address)
    address_info = services.start_ray_node(
        node_ip_address=node_ip_address,
        redis_address=args.redis_address,
        object_manager_ports=[args.object_manager_port],
        num_workers=args.num_workers,
        cleanup=False,
        redirect_output=True,
        num_cpus=args.num_cpus,
        num_gpus=args.num_gpus)
    print(address_info)
    print("\nStarted Ray on this node. If you wish to terminate the processes "
          "that have been started, run\n\n"
          "    ./scripts/stop_ray.sh")
