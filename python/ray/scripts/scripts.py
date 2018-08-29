from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import json
import logging
import os
import subprocess

import ray.services as services
from ray.autoscaler.commands import (attach_cluster, exec_cluster,
                                     create_or_update_cluster, rsync,
                                     teardown_cluster, get_head_node_ip)
import ray.ray_constants as ray_constants
import ray.utils


def check_no_existing_redis_clients(node_ip_address, redis_client):
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

        if ray.utils.decode(info[b"node_ip_address"]) == node_ip_address:
            raise Exception("This Redis instance is already connected to "
                            "clients with this IP address.")


@click.group()
def cli():
    pass


@click.command()
@click.option(
    "--node-ip-address",
    required=False,
    type=str,
    help="the IP address of this node")
@click.option(
    "--redis-address",
    required=False,
    type=str,
    help="the address to use for connecting to Redis")
@click.option(
    "--redis-port",
    required=False,
    type=str,
    help="the port to use for starting Redis")
@click.option(
    "--num-redis-shards",
    required=False,
    type=int,
    help=("the number of additional Redis shards to use in "
          "addition to the primary Redis shard"))
@click.option(
    "--redis-max-clients",
    required=False,
    type=int,
    help=("If provided, attempt to configure Redis with this "
          "maximum number of clients."))
@click.option(
    "--redis-shard-ports",
    required=False,
    type=str,
    help="the port to use for the Redis shards other than the "
    "primary Redis shard")
@click.option(
    "--object-manager-port",
    required=False,
    type=int,
    help="the port to use for starting the object manager")
@click.option(
    "--object-store-memory",
    required=False,
    type=int,
    help="the maximum amount of memory (in bytes) to allow the "
    "object store to use")
@click.option(
    "--num-workers",
    required=False,
    type=int,
    help=("The initial number of workers to start on this node, "
          "note that the local scheduler may start additional "
          "workers. If you wish to control the total number of "
          "concurent tasks, then use --resources instead and "
          "specify the CPU field."))
@click.option(
    "--num-cpus",
    required=False,
    type=int,
    help="the number of CPUs on this node")
@click.option(
    "--num-gpus",
    required=False,
    type=int,
    help="the number of GPUs on this node")
@click.option(
    "--resources",
    required=False,
    default="{}",
    type=str,
    help="a JSON serialized dictionary mapping resource name to "
    "resource quantity")
@click.option(
    "--head",
    is_flag=True,
    default=False,
    help="provide this argument for the head node")
@click.option(
    "--no-ui",
    is_flag=True,
    default=False,
    help="provide this argument if the UI should not be started")
@click.option(
    "--block",
    is_flag=True,
    default=False,
    help="provide this argument to block forever in this command")
@click.option(
    "--plasma-directory",
    required=False,
    type=str,
    help="object store directory for memory mapped files")
@click.option(
    "--huge-pages",
    is_flag=True,
    default=False,
    help="enable support for huge pages in the object store")
@click.option(
    "--autoscaling-config",
    required=False,
    type=str,
    help="the file that contains the autoscaling config")
@click.option(
    "--use-raylet",
    is_flag=True,
    default=None,
    help="use the raylet code path")
@click.option(
    "--no-redirect-worker-output",
    is_flag=True,
    default=False,
    help="do not redirect worker stdout and stderr to files")
@click.option(
    "--no-redirect-output",
    is_flag=True,
    default=False,
    help="do not redirect non-worker stdout and stderr to files")
@click.option(
    "--logging-level",
    required=False,
    default=ray_constants.LOGGER_LEVEL,
    type=str,
    help=ray_constants.LOGGER_LEVEL_HELP)
@click.option(
    "--logging-format",
    required=False,
    default=ray_constants.LOGGER_FORMAT,
    type=str,
    help=ray_constants.LOGGER_FORMAT_HELP)
def start(node_ip_address, redis_address, redis_port, num_redis_shards,
          redis_max_clients, redis_shard_ports, object_manager_port,
          object_store_memory, num_workers, num_cpus, num_gpus, resources,
          head, no_ui, block, plasma_directory, huge_pages, autoscaling_config,
          use_raylet, no_redirect_worker_output, no_redirect_output,
          logging_level, logging_format):
    # Convert hostnames to numerical IP address.
    if node_ip_address is not None:
        node_ip_address = services.address_to_ip(node_ip_address)
    if redis_address is not None:
        redis_address = services.address_to_ip(redis_address)

    level = logging.getLevelName(logging_level.upper())
    logging.basicConfig(level=level, format=logging_format)
    logger = logging.getLogger(__name__)

    if use_raylet is None and os.environ.get("RAY_USE_XRAY") == "1":
        # This environment variable is used in our testing setup.
        logger.info("Detected environment variable 'RAY_USE_XRAY'.")
        use_raylet = True

    try:
        resources = json.loads(resources)
    except Exception as e:
        raise Exception("Unable to parse the --resources argument using "
                        "json.loads. Try using a format like\n\n"
                        "    --resources='{\"CustomResource1\": 3, "
                        "\"CustomReseource2\": 2}'")

    assert "CPU" not in resources, "Use the --num-cpus argument."
    assert "GPU" not in resources, "Use the --num-gpus argument."
    if num_cpus is not None:
        resources["CPU"] = num_cpus
    if num_gpus is not None:
        resources["GPU"] = num_gpus

    if head:
        # Start Ray on the head node.
        if redis_shard_ports is not None:
            redis_shard_ports = redis_shard_ports.split(",")
            # Infer the number of Redis shards from the ports if the number is
            # not provided.
            if num_redis_shards is None:
                num_redis_shards = len(redis_shard_ports)
            # Check that the arguments match.
            if len(redis_shard_ports) != num_redis_shards:
                raise Exception("If --redis-shard-ports is provided, it must "
                                "have the form '6380,6381,6382', and the "
                                "number of ports provided must equal "
                                "--num-redis-shards (which is 1 if not "
                                "provided)")

        if redis_address is not None:
            raise Exception("If --head is passed in, a Redis server will be "
                            "started, so a Redis address should not be "
                            "provided.")

        # Get the node IP address if one is not provided.
        if node_ip_address is None:
            node_ip_address = services.get_node_ip_address()
        logger.info("Using IP address {} for this node."
                    .format(node_ip_address))

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
            redis_shard_ports=redis_shard_ports,
            object_store_memory=object_store_memory,
            num_workers=num_workers,
            cleanup=False,
            redirect_worker_output=not no_redirect_worker_output,
            redirect_output=not no_redirect_output,
            resources=resources,
            num_redis_shards=num_redis_shards,
            redis_max_clients=redis_max_clients,
            redis_protected_mode=False,
            include_webui=(not no_ui),
            plasma_directory=plasma_directory,
            huge_pages=huge_pages,
            autoscaling_config=autoscaling_config,
            use_raylet=use_raylet)
        logger.info(address_info)
        logger.info(
            "\nStarted Ray on this node. You can add additional nodes to "
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
        if redis_shard_ports is not None:
            raise Exception("If --head is not passed in, --redis-shard-ports "
                            "is not allowed")
        if redis_address is None:
            raise Exception("If --head is not passed in, --redis-address must "
                            "be provided.")
        if num_redis_shards is not None:
            raise Exception("If --head is not passed in, --num-redis-shards "
                            "must not be provided.")
        if redis_max_clients is not None:
            raise Exception("If --head is not passed in, --redis-max-clients "
                            "must not be provided.")
        if no_ui:
            raise Exception("If --head is not passed in, the --no-ui flag is "
                            "not relevant.")
        redis_ip_address, redis_port = redis_address.split(":")

        # Wait for the Redis server to be started. And throw an exception if we
        # can't connect to it.
        services.wait_for_redis_to_start(redis_ip_address, int(redis_port))

        # Create a Redis client.
        redis_client = services.create_redis_client(redis_address)

        # Check that the verion information on this node matches the version
        # information that the cluster was started with.
        services.check_version_info(redis_client)

        # Get the node IP address if one is not provided.
        if node_ip_address is None:
            node_ip_address = services.get_node_ip_address(redis_address)
        logger.info("Using IP address {} for this node."
                    .format(node_ip_address))
        # Check that there aren't already Redis clients with the same IP
        # address connected with this Redis instance. This raises an exception
        # if the Redis server already has clients on this node.
        check_no_existing_redis_clients(node_ip_address, redis_client)
        address_info = services.start_ray_node(
            node_ip_address=node_ip_address,
            redis_address=redis_address,
            object_manager_ports=[object_manager_port],
            num_workers=num_workers,
            object_store_memory=object_store_memory,
            cleanup=False,
            redirect_worker_output=not no_redirect_worker_output,
            redirect_output=not no_redirect_output,
            resources=resources,
            plasma_directory=plasma_directory,
            huge_pages=huge_pages,
            use_raylet=use_raylet)
        logger.info(address_info)
        logger.info("\nStarted Ray on this node. If you wish to terminate the "
                    "processes that have been started, run\n\n"
                    "    ray stop")

    if block:
        import time
        while True:
            time.sleep(30)


@click.command()
def stop():
    subprocess.call(
        [
            "killall global_scheduler plasma_store plasma_manager "
            "local_scheduler raylet raylet_monitor"
        ],
        shell=True)

    # Find the PID of the monitor process and kill it.
    subprocess.call(
        [
            "kill $(ps aux | grep monitor.py | grep -v grep | "
            "awk '{ print $2 }') 2> /dev/null"
        ],
        shell=True)

    # Find the PID of the Redis process and kill it.
    subprocess.call(
        [
            "kill $(ps aux | grep redis-server | grep -v grep | "
            "awk '{ print $2 }') 2> /dev/null"
        ],
        shell=True)

    # Find the PIDs of the worker processes and kill them.
    subprocess.call(
        [
            "kill -9 $(ps aux | grep default_worker.py | "
            "grep -v grep | awk '{ print $2 }') 2> /dev/null"
        ],
        shell=True)

    # Find the PID of the Ray log monitor process and kill it.
    subprocess.call(
        [
            "kill $(ps aux | grep log_monitor.py | grep -v grep | "
            "awk '{ print $2 }') 2> /dev/null"
        ],
        shell=True)

    # Find the PID of the jupyter process and kill it.
    try:
        from notebook.notebookapp import list_running_servers
        pids = [
            str(server["pid"]) for server in list_running_servers()
            if "/tmp/raylogs" in server["notebook_dir"]
        ]
        subprocess.call(
            ["kill {} 2> /dev/null".format(" ".join(pids))], shell=True)
    except ImportError:
        pass


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--no-restart",
    is_flag=True,
    default=False,
    help=("Whether to skip restarting Ray services during the update. "
          "This avoids interrupting running jobs."))
@click.option(
    "--restart-only",
    is_flag=True,
    default=False,
    help=("Whether to skip running setup commands and only restart Ray. "
          "This cannot be used with 'no-restart'."))
@click.option(
    "--min-workers",
    required=False,
    type=int,
    help=("Override the configured min worker node count for the cluster."))
@click.option(
    "--max-workers",
    required=False,
    type=int,
    help=("Override the configured max worker node count for the cluster."))
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help=("Override the configured cluster name."))
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help=("Don't ask for confirmation."))
def create_or_update(cluster_config_file, min_workers, max_workers, no_restart,
                     restart_only, yes, cluster_name):
    if restart_only or no_restart:
        assert restart_only != no_restart, "Cannot set both 'restart_only' " \
            "and 'no_restart' at the same time!"
    create_or_update_cluster(cluster_config_file, min_workers, max_workers,
                             no_restart, restart_only, yes, cluster_name)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--workers-only",
    is_flag=True,
    default=False,
    help=("Only destroy the workers."))
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help=("Don't ask for confirmation."))
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help=("Override the configured cluster name."))
def teardown(cluster_config_file, yes, workers_only, cluster_name):
    teardown_cluster(cluster_config_file, yes, workers_only, cluster_name)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help=("Start the cluster if needed."))
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help=("Override the configured cluster name."))
def attach(cluster_config_file, start, cluster_name):
    attach_cluster(cluster_config_file, start, cluster_name)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("source", required=True, type=str)
@click.argument("target", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help=("Override the configured cluster name."))
def rsync_down(cluster_config_file, source, target, cluster_name):
    rsync(cluster_config_file, source, target, cluster_name, down=True)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("source", required=True, type=str)
@click.argument("target", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help=("Override the configured cluster name."))
def rsync_up(cluster_config_file, source, target, cluster_name):
    rsync(cluster_config_file, source, target, cluster_name, down=False)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("cmd", required=True, type=str)
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help=("Stop the cluster after the command finishes running."))
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help=("Start the cluster if needed."))
@click.option(
    "--screen",
    is_flag=True,
    default=False,
    help=("Run the command in a screen."))
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help=("Override the configured cluster name."))
@click.option(
    "--port-forward", required=False, type=int, help=("Port to forward."))
def exec_cmd(cluster_config_file, cmd, screen, stop, start, cluster_name,
             port_forward):
    exec_cluster(cluster_config_file, cmd, screen, stop, start, cluster_name,
                 port_forward)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help=("Override the configured cluster name."))
def get_head_ip(cluster_config_file, cluster_name):
    click.echo(get_head_node_ip(cluster_config_file, cluster_name))


cli.add_command(start)
cli.add_command(stop)
cli.add_command(create_or_update)
cli.add_command(create_or_update, name="up")
cli.add_command(attach)
cli.add_command(exec_cmd, name="exec")
cli.add_command(rsync_down)
cli.add_command(rsync_up)
cli.add_command(teardown)
cli.add_command(teardown, name="down")
cli.add_command(get_head_ip)


def main():
    return cli()


if __name__ == "__main__":
    main()
