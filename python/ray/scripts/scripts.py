from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
from datetime import datetime
import json
import logging
import os
import subprocess
import sys

import ray.services as services
from ray.autoscaler.commands import (
    attach_cluster, exec_cluster, create_or_update_cluster, monitor_cluster,
    rsync, teardown_cluster, get_head_node_ip, kill_node, get_worker_node_ips)
import ray.ray_constants as ray_constants
import ray.utils

logger = logging.getLogger(__name__)


def check_no_existing_redis_clients(node_ip_address, redis_client):
    # The client table prefix must be kept in sync with the file
    # "src/ray/gcs/redis_module/ray_redis_module.cc" where it is defined.
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
def cli(logging_level, logging_format):
    level = logging.getLevelName(logging_level.upper())
    ray.utils.setup_logger(level, logging_format)


@cli.command()
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
    "--redis-password",
    required=False,
    type=str,
    help="If provided, secure Redis ports with this password")
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
    "--node-manager-port",
    required=False,
    type=int,
    help="the port to use for starting the node manager")
@click.option(
    "--object-store-memory",
    required=False,
    type=int,
    help="The amount of memory (in bytes) to start the object store with. "
    "By default, this is capped at 20GB but can be set higher.")
@click.option(
    "--redis-max-memory",
    required=False,
    type=int,
    help="The max amount of memory (in bytes) to allow redis to use. Once the "
    "limit is exceeded, redis will start LRU eviction of entries. This only "
    "applies to the sharded redis tables (task, object, and profile tables). "
    "By default this is capped at 10GB but can be set higher.")
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
    "--include-webui",
    is_flag=True,
    default=False,
    help="provide this argument if the UI should be started")
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
    "--plasma-store-socket-name",
    default=None,
    help="manually specify the socket name of the plasma store")
@click.option(
    "--raylet-socket-name",
    default=None,
    help="manually specify the socket path of the raylet process")
@click.option(
    "--temp-dir",
    default=None,
    help="manually specify the root temporary dir of the Ray process")
@click.option(
    "--include-java",
    is_flag=True,
    default=None,
    help="Enable Java worker support.")
@click.option(
    "--java-worker-options",
    required=False,
    default=None,
    type=str,
    help="Overwrite the options to start Java workers.")
@click.option(
    "--internal-config",
    default=None,
    type=str,
    help="Do NOT use this. This is for debugging/development purposes ONLY.")
@click.option(
    "--load-code-from-local",
    is_flag=True,
    default=False,
    help="Specify whether load code from local file or GCS serialization.")
def start(node_ip_address, redis_address, redis_port, num_redis_shards,
          redis_max_clients, redis_password, redis_shard_ports,
          object_manager_port, node_manager_port, object_store_memory,
          redis_max_memory, num_cpus, num_gpus, resources, head, include_webui,
          block, plasma_directory, huge_pages, autoscaling_config,
          no_redirect_worker_output, no_redirect_output,
          plasma_store_socket_name, raylet_socket_name, temp_dir, include_java,
          java_worker_options, load_code_from_local, internal_config):
    # Convert hostnames to numerical IP address.
    if node_ip_address is not None:
        node_ip_address = services.address_to_ip(node_ip_address)
    if redis_address is not None:
        redis_address = services.address_to_ip(redis_address)

    try:
        resources = json.loads(resources)
    except Exception:
        raise Exception("Unable to parse the --resources argument using "
                        "json.loads. Try using a format like\n\n"
                        "    --resources='{\"CustomResource1\": 3, "
                        "\"CustomReseource2\": 2}'")

    redirect_worker_output = None if not no_redirect_worker_output else True
    redirect_output = None if not no_redirect_output else True
    ray_params = ray.parameter.RayParams(
        node_ip_address=node_ip_address,
        object_manager_port=object_manager_port,
        node_manager_port=node_manager_port,
        object_store_memory=object_store_memory,
        redis_password=redis_password,
        redirect_worker_output=redirect_worker_output,
        redirect_output=redirect_output,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=resources,
        plasma_directory=plasma_directory,
        huge_pages=huge_pages,
        plasma_store_socket_name=plasma_store_socket_name,
        raylet_socket_name=raylet_socket_name,
        temp_dir=temp_dir,
        include_java=include_java,
        include_webui=include_webui,
        java_worker_options=java_worker_options,
        load_code_from_local=load_code_from_local,
        _internal_config=internal_config)

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
        ray_params.update_if_absent(
            node_ip_address=services.get_node_ip_address())
        logger.info("Using IP address {} for this node.".format(
            ray_params.node_ip_address))
        ray_params.update_if_absent(
            redis_port=redis_port,
            redis_shard_ports=redis_shard_ports,
            redis_max_memory=redis_max_memory,
            num_redis_shards=num_redis_shards,
            redis_max_clients=redis_max_clients,
            autoscaling_config=autoscaling_config,
            include_java=False,
        )

        node = ray.node.Node(ray_params, head=True, shutdown_at_exit=False)
        redis_address = node.redis_address

        logger.info(
            "\nStarted Ray on this node. You can add additional nodes to "
            "the cluster by calling\n\n"
            "    ray start --redis-address {}{}{}\n\n"
            "from the node you wish to add. You can connect a driver to the "
            "cluster from Python by running\n\n"
            "    import ray\n"
            "    ray.init(redis_address=\"{}{}{}\")\n\n"
            "If you have trouble connecting from a different machine, check "
            "that your firewall is configured properly. If you wish to "
            "terminate the processes that have been started, run\n\n"
            "    ray stop".format(
                redis_address, " --redis-password "
                if redis_password else "", redis_password if redis_password
                else "", redis_address, "\", redis_password=\""
                if redis_password else "", redis_password
                if redis_password else ""))
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
        if include_webui:
            raise Exception("If --head is not passed in, the --include-webui "
                            "flag is not relevant.")
        if include_java is not None:
            raise ValueError("--include-java should only be set for the head "
                             "node.")

        redis_ip_address, redis_port = redis_address.split(":")

        # Wait for the Redis server to be started. And throw an exception if we
        # can't connect to it.
        services.wait_for_redis_to_start(
            redis_ip_address, int(redis_port), password=redis_password)

        # Create a Redis client.
        redis_client = services.create_redis_client(
            redis_address, password=redis_password)

        # Check that the verion information on this node matches the version
        # information that the cluster was started with.
        services.check_version_info(redis_client)

        # Get the node IP address if one is not provided.
        ray_params.update_if_absent(
            node_ip_address=services.get_node_ip_address(redis_address))
        logger.info("Using IP address {} for this node.".format(
            ray_params.node_ip_address))
        # Check that there aren't already Redis clients with the same IP
        # address connected with this Redis instance. This raises an exception
        # if the Redis server already has clients on this node.
        check_no_existing_redis_clients(ray_params.node_ip_address,
                                        redis_client)
        ray_params.update(redis_address=redis_address)
        node = ray.node.Node(ray_params, head=False, shutdown_at_exit=False)
        logger.info("\nStarted Ray on this node. If you wish to terminate the "
                    "processes that have been started, run\n\n"
                    "    ray stop")

    if block:
        import time
        while True:
            time.sleep(30)


@cli.command()
def stop():
    # Note that raylet needs to exit before object store, otherwise
    # it cannot exit gracefully.
    processes_to_kill = [
        "raylet",
        "plasma_store_server",
        "raylet_monitor",
        "monitor.py",
        "redis-server",
        "default_worker.py",  # Python worker.
        " ray_",  # Python worker.
        "org.ray.runtime.runner.worker.DefaultWorker",  # Java worker.
        "log_monitor.py",
        "reporter.py",
        "dashboard.py",
    ]

    for process in processes_to_kill:
        command = ("kill -9 $(ps aux | grep '" + process +
                   "' | grep -v grep | " + "awk '{ print $2 }') 2> /dev/null")
        subprocess.call([command], shell=True)

    # Find the PID of the jupyter process and kill it.
    try:
        from notebook.notebookapp import list_running_servers
        pids = [
            str(server["pid"]) for server in list_running_servers()
            if "/tmp/ray" in server["notebook_dir"]
        ]
        subprocess.call(
            ["kill -9 {} 2> /dev/null".format(" ".join(pids))], shell=True)
    except ImportError:
        pass
    except Exception:
        logger.exception("Error shutting down jupyter")


@cli.command()
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
    help="Override the configured min worker node count for the cluster.")
@click.option(
    "--max-workers",
    required=False,
    type=int,
    help="Override the configured max worker node count for the cluster.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
def create_or_update(cluster_config_file, min_workers, max_workers, no_restart,
                     restart_only, yes, cluster_name):
    """Create or update a Ray cluster."""
    if restart_only or no_restart:
        assert restart_only != no_restart, "Cannot set both 'restart_only' " \
            "and 'no_restart' at the same time!"
    create_or_update_cluster(cluster_config_file, min_workers, max_workers,
                             no_restart, restart_only, yes, cluster_name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--workers-only",
    is_flag=True,
    default=False,
    help="Only destroy the workers.")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def teardown(cluster_config_file, yes, workers_only, cluster_name):
    """Tear down the Ray cluster."""
    teardown_cluster(cluster_config_file, yes, workers_only, cluster_name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--hard",
    is_flag=True,
    default=False,
    help="Terminates the node via node provider (defaults to a 'soft kill'"
    " which terminates Ray but does not actually delete the instances).")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def kill_random_node(cluster_config_file, yes, hard, cluster_name):
    """Kills a random Ray node. For testing purposes only."""
    click.echo("Killed node with IP " +
               kill_node(cluster_config_file, yes, hard, cluster_name))


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--lines",
    required=False,
    default=100,
    type=int,
    help="Number of lines to tail.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def monitor(cluster_config_file, lines, cluster_name):
    """Runs `tail -n [lines] -f /tmp/ray/session_*/logs/monitor*` on head."""
    monitor_cluster(cluster_config_file, lines, cluster_name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help="Start the cluster if needed.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--new", "-N", is_flag=True, help="Force creation of a new screen.")
def attach(cluster_config_file, start, tmux, cluster_name, new):
    attach_cluster(cluster_config_file, start, tmux, cluster_name, new)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("source", required=False, type=str)
@click.argument("target", required=False, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def rsync_down(cluster_config_file, source, target, cluster_name):
    rsync(cluster_config_file, source, target, cluster_name, down=True)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("source", required=False, type=str)
@click.argument("target", required=False, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def rsync_up(cluster_config_file, source, target, cluster_name):
    rsync(cluster_config_file, source, target, cluster_name, down=False)


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--docker",
    is_flag=True,
    default=False,
    help="Runs command in the docker container specified in cluster_config.")
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Stop the cluster after the command finishes running.")
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help="Start the cluster if needed.")
@click.option(
    "--screen",
    is_flag=True,
    default=False,
    help="Run the command in a screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--port-forward", required=False, type=int, help="Port to forward.")
@click.argument("script", required=True, type=str)
@click.option("--args", required=False, type=str, help="Script args.")
def submit(cluster_config_file, docker, screen, tmux, stop, start,
           cluster_name, port_forward, script, args):
    """Uploads and runs a script on the specified cluster.

    The script is automatically synced to the following location:

        os.path.join("~", os.path.basename(script))

    Example:
        >>> ray submit [CLUSTER.YAML] experiment.py --args="--smoke-test"
    """
    assert not (screen and tmux), "Can specify only one of `screen` or `tmux`."

    if start:
        create_or_update_cluster(cluster_config_file, None, None, False, False,
                                 True, cluster_name)

    target = os.path.join("~", os.path.basename(script))
    rsync(cluster_config_file, script, target, cluster_name, down=False)

    command_parts = ["python", target]
    if args is not None:
        command_parts += [args]
    cmd = " ".join(command_parts)
    exec_cluster(cluster_config_file, cmd, docker, screen, tmux, stop, False,
                 cluster_name, port_forward)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("cmd", required=True, type=str)
@click.option(
    "--docker",
    is_flag=True,
    default=False,
    help="Runs command in the docker container specified in cluster_config.")
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Stop the cluster after the command finishes running.")
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help="Start the cluster if needed.")
@click.option(
    "--screen",
    is_flag=True,
    default=False,
    help="Run the command in a screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--port-forward", required=False, type=int, help="Port to forward.")
def exec_cmd(cluster_config_file, cmd, docker, screen, tmux, stop, start,
             cluster_name, port_forward):
    exec_cluster(cluster_config_file, cmd, docker, screen, tmux, stop, start,
                 cluster_name, port_forward)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def get_head_ip(cluster_config_file, cluster_name):
    click.echo(get_head_node_ip(cluster_config_file, cluster_name))


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def get_worker_ips(cluster_config_file, cluster_name):
    worker_ips = get_worker_node_ips(cluster_config_file, cluster_name)
    click.echo("\n".join(worker_ips))


@cli.command()
@click.argument("command", required=True, type=str)
@click.option(
    "--dry",
    is_flag=True,
    default=False,
    help="Print actions instead of running them.")
def session(command, dry):
    ray.projects.load_project(os.getcwd())


@cli.command()
def stack():
    COMMAND = """
pyspy=`which py-spy`
if [ ! -e "$pyspy" ]; then
    echo "ERROR: Please 'pip install py-spy' (or ray[debug]) first"
    exit 1
fi
# Set IFS to iterate over lines instead of over words.
export IFS="
"
# Call sudo to prompt for password before anything has been printed.
sudo true
workers=$(
    ps aux | grep -E ' ray_|default_worker.py' | grep -v grep
)
for worker in $workers; do
    echo "Stack dump for $worker";
    pid=`echo $worker | awk '{print $2}'`;
    sudo $pyspy --pid $pid --dump;
    echo;
done
    """
    subprocess.call(COMMAND, shell=True)


@cli.command()
@click.option(
    "--redis-address",
    required=False,
    type=str,
    help="Override the redis address to connect to.")
def timeline(redis_address):
    if not redis_address:
        import psutil
        pids = psutil.pids()
        redis_addresses = set()
        for pid in pids:
            try:
                proc = psutil.Process(pid)
                for arglist in proc.cmdline():
                    for arg in arglist.split(" "):
                        if arg.startswith("--redis-address="):
                            addr = arg.split("=")[1]
                            redis_addresses.add(addr)
            except psutil.AccessDenied:
                pass
            except psutil.NoSuchProcess:
                pass
        if len(redis_addresses) > 1:
            logger.info(
                "Found multiple active Ray instances: {}. ".format(
                    redis_addresses) +
                "Please specify the one to connect to with --redis-address.")
            sys.exit(1)
        elif not redis_addresses:
            logger.info(
                "Could not find any running Ray instance. "
                "Please specify the one to connect to with --redis-address.")
            sys.exit(1)
        redis_address = redis_addresses.pop()
    logger.info("Connecting to Ray instance at {}.".format(redis_address))
    ray.init(redis_address=redis_address)
    time = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    filename = "/tmp/ray-timeline-{}.json".format(time)
    ray.timeline(filename=filename)
    size = os.path.getsize(filename)
    logger.info("Trace file written to {} ({} bytes).".format(filename, size))
    logger.info(
        "You can open this with chrome://tracing in the Chrome browser.")


cli.add_command(start)
cli.add_command(stop)
cli.add_command(create_or_update, name="up")
cli.add_command(attach)
cli.add_command(exec_cmd, name="exec")
cli.add_command(rsync_down, name="rsync_down")
cli.add_command(rsync_up, name="rsync_up")
cli.add_command(submit)
cli.add_command(teardown)
cli.add_command(teardown, name="down")
cli.add_command(kill_random_node)
cli.add_command(get_head_ip, name="get_head_ip")
cli.add_command(get_worker_ips)
cli.add_command(session)
cli.add_command(stack)
cli.add_command(timeline)


def main():
    return cli()


if __name__ == "__main__":
    main()
