from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import redis
import subprocess

import ray.services as services


def check_no_existing_redis_clients(node_ip_address, redis_address):
    redis_ip_address, redis_port = redis_address.split(":")
    redis_client = redis.StrictRedis(host=redis_ip_address,
                                     port=int(redis_port))
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
            raise Exception("This Redis instance is already connected to "
                            "clients with this IP address.")


@click.group()
def cli():
    pass


@click.command()
@click.option("--node-ip-address", required=False, type=str,
              help="the IP address of this node")
@click.option("--redis-address", required=False, type=str,
              help="the address to use for connecting to Redis")
@click.option("--redis-port", required=False, type=str,
              help="the port to use for starting Redis")
@click.option("--num-redis-shards", required=False, type=int,
              help=("the number of additional Redis shards to use in "
                    "addition to the primary Redis shard"))
@click.option("--num-local-redis-shards", required=False, type=int,
              help=("the number of Redis shards that this nodes should start"))
@click.option("--object-manager-port", required=False, type=int,
              help="the port to use for starting the object manager")
@click.option("--num-workers", required=False, type=int,
              help="the initial number of workers to start on this node")
@click.option("--num-cpus", required=False, type=int,
              help="the number of CPUs on this node")
@click.option("--num-gpus", required=False, type=int,
              help="the number of GPUs on this node")
@click.option("--num-custom-resource", required=False, type=int,
              help="the amount of a user-defined custom resource on this node")
@click.option("--head", is_flag=True, default=False,
              help="provide this argument for the head node")
@click.option("--block", is_flag=True, default=False,
              help="provide this argument to block forever in this command")
def start(node_ip_address, redis_address, redis_port, num_redis_shards,
          object_manager_port, num_workers, num_cpus, num_gpus,
          num_custom_resource, head, block, num_local_redis_shards):
    # Note that we redirect stdout and stderr to /dev/null because otherwise
    # attempts to print may cause exceptions if a process is started inside of
    # an SSH connection and the SSH connection dies. TODO(rkn): This is a
    # temporary fix. We should actually redirect stdout and stderr to Redis in
    # some way.

    if head:
        # Start Ray on the head node.
        if redis_address is not None:
            raise Exception("If --head is passed in, a Redis server will be "
                            "started, so a Redis address should not be "
                            "provided.")

        # Get the node IP address if one is not provided.
        if node_ip_address is None:
            node_ip_address = services.get_node_ip_address()
        print("Using IP address {} for this node.".format(node_ip_address))

        address_info = {}
        # Use the provided object manager port if there is one.
        if object_manager_port is not None:
            address_info["object_manager_ports"] = [object_manager_port]
        if address_info == {}:
            address_info = None

        address_info = services.start_ray_head(
            address_info=address_info,
            node_ip_address=node_ip_address,
            redis_port=redis_port,
            num_workers=num_workers,
            cleanup=False,
            redirect_output=True,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            num_custom_resource=num_custom_resource,
            num_redis_shards=num_redis_shards,
            num_local_redis_shards=num_local_redis_shards)
        print(address_info)
        print("\nStarted Ray on this node. You can add additional nodes to "
              "the cluster by calling\n\n"
              "    ray start --redis-address {}\n\n"
              "from the node you wish to add. You can connect a driver to the "
              "cluster from Python by running\n\n"
              "    import ray\n"
              "    ray.init(redis_address=\"{}\")\n\n"
              "If you have trouble connecting from a different machine, check "
              "that your firewall is configured properly. If you wish to "
              "terminate the processes that have been started, run\n\n"
              "    ray stop".format(address_info["redis_address"],
                                    address_info["redis_address"]))
    else:
        # Start Ray on a non-head node.
        if redis_port is not None:
            raise Exception("If --head is not passed in, --redis-port is not "
                            "allowed")
        if redis_address is None:
            raise Exception("If --head is not passed in, --redis-address must "
                            "be provided.")
        redis_ip_address, redis_port = redis_address.split(":")
        # Wait for the Redis server to be started. And throw an exception if we
        # can't connect to it.
        services.wait_for_redis_to_start(redis_ip_address, int(redis_port))
        # Get the node IP address if one is not provided.
        if node_ip_address is None:
            node_ip_address = services.get_node_ip_address(redis_address)
        print("Using IP address {} for this node.".format(node_ip_address))
        # Check that there aren't already Redis clients with the same IP
        # address connected with this Redis instance. This raises an exception
        # if the Redis server already has clients on this node.
        check_no_existing_redis_clients(node_ip_address, redis_address)
        address_info = services.start_ray_node(
            node_ip_address=node_ip_address,
            redis_address=redis_address,
            object_manager_ports=[object_manager_port],
            num_workers=num_workers,
            cleanup=False,
            redirect_output=True,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            num_custom_resource=num_custom_resource,
            num_redis_shards=num_redis_shards,
            num_local_redis_shards=num_local_redis_shards)
        print(address_info)
        print("\nStarted Ray on this node. If you wish to terminate the "
              "processes that have been started, run\n\n"
              "    ray stop")

    if block:
        import time
        while True:
            time.sleep(30)


@click.command()
def stop():
    subprocess.call(["killall global_scheduler plasma_store plasma_manager "
                     "local_scheduler"], shell=True)

    # Find the PID of the monitor process and kill it.
    subprocess.call(["kill $(ps aux | grep monitor.py | grep -v grep | "
                     "awk '{ print $2 }') 2> /dev/null"], shell=True)

    # Find the PID of the Redis process and kill it.
    subprocess.call(["kill $(ps aux | grep redis-server | grep -v grep | "
                     "awk '{ print $2 }') 2> /dev/null"], shell=True)

    # Find the PIDs of the worker processes and kill them.
    subprocess.call(["kill -9 $(ps aux | grep default_worker.py | "
                     "grep -v grep | awk '{ print $2 }') 2> /dev/null"],
                    shell=True)

    # Find the PID of the Ray log monitor process and kill it.
    subprocess.call(["kill $(ps aux | grep log_monitor.py | grep -v grep | "
                     "awk '{ print $2 }') 2> /dev/null"], shell=True)

    # Find the PID of the jupyter process and kill it.
    subprocess.call(["kill $(ps aux | grep jupyter | grep -v grep | "
                     "awk '{ print $2 }') 2> /dev/null"], shell=True)


cli.add_command(start)
cli.add_command(stop)


def main():
    return cli()


if __name__ == "__main__":
    main()
