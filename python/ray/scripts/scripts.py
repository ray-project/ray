import click
import copy
from datetime import datetime
import json
import logging
import os
import subprocess
import sys
import time
import urllib
import urllib.parse
import yaml

import ray
import psutil
import ray.services as services
from ray.autoscaler.commands import (
    attach_cluster, exec_cluster, create_or_update_cluster, monitor_cluster,
    rsync, teardown_cluster, get_head_node_ip, kill_node, get_worker_node_ips,
    debug_status, RUN_ENV_TYPES)
import ray.ray_constants as ray_constants
import ray.utils
from ray.projects.scripts import project_cli, session_cli

from ray.autoscaler.subprocess_output_util import set_output_redirected
from ray.autoscaler.cli_logger import cli_logger
import colorful as cf

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


logging_options = [
    click.option(
        "--log-new-style/--log-old-style",
        is_flag=True,
        default=False,
        envvar="RAY_LOG_NEWSTYLE",
        help=("Whether to use the old or the new CLI UX. "
              "The new UX supports colored, formatted output and was "
              "designed to display only the most important information for "
              "human users. The old UX uses the standard `logging` module. "
              "It is most suitable for writing to a file and will include "
              "timestamps and message level (ERROR/WARNING/INFO).")),
    click.option(
        "--log-color",
        required=False,
        type=click.Choice(["auto", "false", "true"], case_sensitive=False),
        default="auto",
        help=("Use color logging. "
              "Valid values are: auto (if stdout is a tty), true, false.")),
    click.option("-v", "--verbose", count=True)
]


def add_click_options(options):
    def wrapper(f):
        for option in reversed(logging_options):
            f = option(f)
        return f

    return wrapper


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
@click.version_option()
def cli(logging_level, logging_format):
    level = logging.getLevelName(logging_level.upper())
    ray.utils.setup_logger(level, logging_format)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--port",
    "-p",
    required=False,
    type=int,
    default=8265,
    help="The local port to forward to the dashboard")
@click.option(
    "--remote-port",
    required=False,
    type=int,
    default=8265,
    help="The remote port your dashboard runs on")
def dashboard(cluster_config_file, cluster_name, port, remote_port):
    """Port-forward a Ray cluster's dashboard to the local machine."""
    # Sleeping in a loop is preferable to `sleep infinity` because the latter
    # only works on linux.
    if port:
        dashboard_port = port
    else:
        dashboard_port = remote_port

    port_taken = True

    # Find the first open port sequentially from `remote_port`.
    while port_taken:
        try:
            port_forward = [
                (dashboard_port, remote_port),
            ]
            click.echo(("Attempting to establish dashboard locally at"
                        " localhost:{} connected to"
                        " remote port {}").format(dashboard_port, remote_port))
            # We want to probe with a no-op that returns quickly to avoid
            # exceptions caused by network errors.
            exec_cluster(
                cluster_config_file,
                override_cluster_name=cluster_name,
                port_forward=port_forward)
            port_taken = False
        except Exception:
            click.echo("Failed to forward dashboard, trying a new port...")
            port_taken = True
            dashboard_port += 1
            pass


@cli.command()
@click.option(
    "--node-ip-address",
    required=False,
    type=str,
    help="the IP address of this node")
@click.option(
    "--redis-address", required=False, type=str, help="same as --address")
@click.option(
    "--address", required=False, type=str, help="the address to use for Ray")
@click.option(
    "--redis-port",
    required=False,
    type=str,
    help="(DEPRECATED) the port to use for starting redis. "
    "Please use --port instead now.")
@click.option(
    "--port",
    required=False,
    type=str,
    help="the port of the head ray process. If not provided, tries to use "
    "{0}, falling back to a random port if {0} is "
    "not available".format(ray_constants.DEFAULT_PORT))
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
    default=ray_constants.REDIS_DEFAULT_PASSWORD,
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
    "--gcs-server-port",
    required=False,
    type=int,
    help="Port number for the GCS server.")
@click.option(
    "--min-worker-port",
    required=False,
    type=int,
    default=10000,
    help="the lowest port number that workers will bind on. If not set, "
    "random ports will be chosen.")
@click.option(
    "--max-worker-port",
    required=False,
    type=int,
    default=10999,
    help="the highest port number that workers will bind on. If set, "
    "'--min-worker-port' must also be set.")
@click.option(
    "--memory",
    required=False,
    type=int,
    help="The amount of memory (in bytes) to make available to workers. "
    "By default, this is set to the available memory on the node.")
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
    default=None,
    type=bool,
    help="provide this argument if the UI should be started "
    "(DEPRECATED: please use --include-dashboard.")
@click.option(
    "--webui-host",
    required=False,
    default="localhost",
    help="the host to bind the dashboard server to, either localhost "
    "(127.0.0.1) or 0.0.0.0 (available from all interfaces). By default,"
    " this is localhost."
    " (DEPRECATED: please use --dashboard-host)")
@click.option(
    "--include-dashboard",
    default=None,
    type=bool,
    help="provide this argument to start the Ray dashboard GUI")
@click.option(
    "--dashboard-host",
    required=False,
    default="localhost",
    help="the host to bind the dashboard server to, either localhost "
    "(127.0.0.1) or 0.0.0.0 (available from all interfaces). By default, this"
    "is localhost.")
@click.option(
    "--dashboard-port",
    required=False,
    type=int,
    default=ray_constants.DEFAULT_DASHBOARD_PORT,
    help="the port to bind the dashboard server to--defaults to {}".format(
        ray_constants.DEFAULT_DASHBOARD_PORT))
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
    type=json.loads,
    help="Do NOT use this. This is for debugging/development purposes ONLY.")
@click.option(
    "--load-code-from-local",
    is_flag=True,
    default=False,
    help="Specify whether load code from local file or GCS serialization.")
@click.option(
    "--lru-evict",
    is_flag=True,
    default=False,
    help="Specify whether LRU evict will be used for this cluster.")
@click.option(
    "--enable-object-reconstruction",
    is_flag=True,
    default=False,
    help="Specify whether object reconstruction will be used for this cluster."
)
@click.option(
    "--metrics-export-port",
    type=int,
    default=None,
    help="the port to use to expose Ray metrics through a "
    "Prometheus endpoint.")
@add_click_options(logging_options)
def start(node_ip_address, redis_address, address, redis_port, port,
          num_redis_shards, redis_max_clients, redis_password,
          redis_shard_ports, object_manager_port, node_manager_port,
          gcs_server_port, min_worker_port, max_worker_port, memory,
          object_store_memory, redis_max_memory, num_cpus, num_gpus, resources,
          head, include_webui, webui_host, include_dashboard, dashboard_host,
          dashboard_port, block, plasma_directory, huge_pages,
          autoscaling_config, no_redirect_worker_output, no_redirect_output,
          plasma_store_socket_name, raylet_socket_name, temp_dir, include_java,
          java_worker_options, load_code_from_local, internal_config,
          lru_evict, enable_object_reconstruction, metrics_export_port,
          log_new_style, log_color, verbose):
    """Start Ray processes manually on the local machine."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    if gcs_server_port and not head:
        raise ValueError(
            "gcs_server_port can be only assigned when you specify --head.")

    if redis_address is not None:
        cli_logger.abort("{} is deprecated. Use {} instead.",
                         cf.bold("--redis-address"), cf.bold("--address"))

        raise DeprecationWarning("The --redis-address argument is "
                                 "deprecated. Please use --address instead.")
    if redis_port is not None:
        cli_logger.warning("{} is being deprecated. Use {} instead.",
                           cf.bold("--redis-port"), cf.bold("--port"))
        cli_logger.old_warning(
            logger, "The --redis-port argument will be deprecated soon. "
            "Please use --port instead.")
        if port is not None and port != redis_port:
            cli_logger.abort(
                "Incompatible values for {} and {}. Use only {} instead.",
                cf.bold("--port"), cf.bold("--redis-port"), cf.bold("--port"))

            raise ValueError("Cannot specify both --port and --redis-port "
                             "as port is a rename of deprecated redis-port")
    if include_webui is not None:
        cli_logger.warning("{} is being deprecated. Use {} instead.",
                           cf.bold("--include-webui"),
                           cf.bold("--include-dashboard"))
        cli_logger.old_warning(
            logger, "The --include-webui argument will be deprecated soon"
            "Please use --include-dashboard instead.")
        if include_dashboard is not None:
            include_dashboard = include_webui

    dashboard_host_default = "localhost"
    if webui_host != dashboard_host_default:
        cli_logger.warning("{} is being deprecated. Use {} instead.",
                           cf.bold("--webui-host"),
                           cf.bold("--dashboard-host"))
        cli_logger.old_warning(
            logger, "The --webui-host argument will be deprecated"
            " soon. Please use --dashboard-host instead.")
        if webui_host != dashboard_host and dashboard_host != "localhost":
            cli_logger.abort(
                "Incompatible values for {} and {}. Use only {} instead.",
                cf.bold("--dashboard-host"), cf.bold("--webui-host"),
                cf.bold("--dashboard-host"))

            raise ValueError(
                "Cannot specify both --webui-host and --dashboard-host,"
                " please specify only the latter")
        else:
            dashboard_host = webui_host

    # Convert hostnames to numerical IP address.
    if node_ip_address is not None:
        node_ip_address = services.address_to_ip(node_ip_address)

    if redis_address is not None or address is not None:
        (redis_address, redis_address_ip,
         redis_address_port) = services.validate_redis_address(
             address, redis_address)

    try:
        resources = json.loads(resources)
    except Exception:
        cli_logger.error("`{}` is not a valid JSON string.",
                         cf.bold("--resources"))
        cli_logger.abort(
            "Valid values look like this: `{}`",
            cf.bold("--resources='\"CustomResource3\": 1, "
                    "\"CustomResource2\": 2}'"))

        raise Exception("Unable to parse the --resources argument using "
                        "json.loads. Try using a format like\n\n"
                        "    --resources='{\"CustomResource1\": 3, "
                        "\"CustomReseource2\": 2}'")

    redirect_worker_output = None if not no_redirect_worker_output else True
    redirect_output = None if not no_redirect_output else True
    ray_params = ray.parameter.RayParams(
        node_ip_address=node_ip_address,
        min_worker_port=min_worker_port,
        max_worker_port=max_worker_port,
        object_manager_port=object_manager_port,
        node_manager_port=node_manager_port,
        gcs_server_port=gcs_server_port,
        memory=memory,
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
        include_dashboard=include_dashboard,
        dashboard_host=dashboard_host,
        dashboard_port=dashboard_port,
        java_worker_options=java_worker_options,
        load_code_from_local=load_code_from_local,
        _internal_config=internal_config,
        lru_evict=lru_evict,
        enable_object_reconstruction=enable_object_reconstruction,
        metrics_export_port=metrics_export_port)
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
                cli_logger.error(
                    "`{}` must be a comma-separated list of ports, "
                    "with length equal to `{}` (which defaults to {})",
                    cf.bold("--redis-shard-ports"),
                    cf.bold("--num-redis-shards"), cf.bold("1"))
                cli_logger.abort(
                    "Example: `{}`",
                    cf.bold("--num-redis-shards 3 "
                            "--redis_shard_ports 6380,6381,6382"))

                raise Exception("If --redis-shard-ports is provided, it must "
                                "have the form '6380,6381,6382', and the "
                                "number of ports provided must equal "
                                "--num-redis-shards (which is 1 if not "
                                "provided)")

        if redis_address is not None:
            cli_logger.abort(
                "`{}` starts a new Redis server, `{}` should not be set.",
                cf.bold("--head"), cf.bold("--address"))

            raise Exception("If --head is passed in, a Redis server will be "
                            "started, so a Redis address should not be "
                            "provided.")

        # Get the node IP address if one is not provided.
        ray_params.update_if_absent(
            node_ip_address=services.get_node_ip_address())
        cli_logger.labeled_value("Local node IP", ray_params.node_ip_address)
        cli_logger.old_info(logger, "Using IP address {} for this node.",
                            ray_params.node_ip_address)
        ray_params.update_if_absent(
            redis_port=port or redis_port,
            redis_shard_ports=redis_shard_ports,
            redis_max_memory=redis_max_memory,
            num_redis_shards=num_redis_shards,
            redis_max_clients=redis_max_clients,
            autoscaling_config=autoscaling_config,
            include_java=False,
        )

        node = ray.node.Node(
            ray_params, head=True, shutdown_at_exit=block, spawn_reaper=block)
        redis_address = node.redis_address

        # new-style CLI UX (--log-new-style)
        # this is a noop if that flag is not set, so the old logger calls
        # are still in place
        cli_logger.newline()
        startup_msg = "Ray runtime started."
        cli_logger.success("-" * len(startup_msg))
        cli_logger.success(startup_msg)
        cli_logger.success("-" * len(startup_msg))
        cli_logger.newline()
        with cli_logger.group("Next steps"):
            cli_logger.print(
                "To connect to this Ray runtime from another node, run")
            cli_logger.print(
                cf.bold("  ray start --address='{}'{}"), redis_address,
                " --redis-password='{}'".format(redis_password)
                if redis_password else "")
            cli_logger.newline()
            cli_logger.print("Alternatively, use the following Python code:")
            with cli_logger.indented():
                with cf.with_style("monokai") as c:
                    cli_logger.print("{} ray", c.magenta("import"))
                    cli_logger.print(
                        "ray{}init(address{}{}{})", c.magenta("."),
                        c.magenta("="), c.yellow("'auto'"),
                        ", redis_password{}{}".format(
                            c.magenta("="),
                            c.yellow("'" + redis_password + "'"))
                        if redis_password else "")
            cli_logger.newline()
            cli_logger.print(
                cf.underlined("If connection fails, check your "
                              "firewall settings other "
                              "network configuration."))
            cli_logger.newline()
            cli_logger.print("To terminate the Ray runtime, run")
            cli_logger.print(cf.bold("  ray stop"))

        cli_logger.old_info(
            logger,
            "\nStarted Ray on this node. You can add additional nodes to "
            "the cluster by calling\n\n"
            "    ray start --address='{}'{}\n\n"
            "from the node you wish to add. You can connect a driver to the "
            "cluster from Python by running\n\n"
            "    import ray\n"
            "    ray.init(address='auto'{})\n\n"
            "If you have trouble connecting from a different machine, check "
            "that your firewall is configured properly. If you wish to "
            "terminate the processes that have been started, run\n\n"
            "    ray stop".format(
                redis_address, " --redis-password='" + redis_password + "'"
                if redis_password else "",
                ", redis_password='" + redis_password + "'"
                if redis_password else ""))
    else:
        # Start Ray on a non-head node.
        if not (redis_port is None and port is None):
            cli_logger.abort("`{}/{}` should not be specified without `{}`.",
                             cf.bold("--port"), cf.bold("--redis-port"),
                             cf.bold("--head"))

            raise Exception(
                "If --head is not passed in, --port and --redis-port are not "
                "allowed.")
        if redis_shard_ports is not None:
            cli_logger.abort("`{}` should not be specified without `{}`.",
                             cf.bold("--redis-shard-ports"), cf.bold("--head"))

            raise Exception("If --head is not passed in, --redis-shard-ports "
                            "is not allowed.")
        if redis_address is None:
            cli_logger.abort("`{}` is required unless starting with `{}`.",
                             cf.bold("--address"), cf.bold("--head"))

            raise Exception("If --head is not passed in, --address must "
                            "be provided.")
        if num_redis_shards is not None:
            cli_logger.abort("`{}` should not be specified without `{}`.",
                             cf.bold("--num-redis-shards"), cf.bold("--head"))

            raise Exception("If --head is not passed in, --num-redis-shards "
                            "must not be provided.")
        if redis_max_clients is not None:
            cli_logger.abort("`{}` should not be specified without `{}`.",
                             cf.bold("--redis-max-clients"), cf.bold("--head"))

            raise Exception("If --head is not passed in, --redis-max-clients "
                            "must not be provided.")
        if include_webui:
            cli_logger.abort("`{}` should not be specified without `{}`.",
                             cf.bold("--include-web-ui"), cf.bold("--head"))

            raise Exception("If --head is not passed in, the --include-webui"
                            "flag is not relevant.")
        if include_dashboard:
            cli_logger.abort("`{}` should not be specified without `{}`.",
                             cf.bold("--include-dashboard"), cf.bold("--head"))

            raise ValueError(
                "If --head is not passed in, the --include-dashboard"
                "flag is not relevant.")
        if include_java is not None:
            cli_logger.abort("`{}` should not be specified without `{}`.",
                             cf.bold("--include-java"), cf.bold("--head"))

            raise ValueError("--include-java should only be set for the head "
                             "node.")

        # Wait for the Redis server to be started. And throw an exception if we
        # can't connect to it.
        services.wait_for_redis_to_start(
            redis_address_ip, redis_address_port, password=redis_password)

        # Create a Redis client.
        redis_client = services.create_redis_client(
            redis_address, password=redis_password)

        # Check that the version information on this node matches the version
        # information that the cluster was started with.
        services.check_version_info(redis_client)

        # Get the node IP address if one is not provided.
        ray_params.update_if_absent(
            node_ip_address=services.get_node_ip_address(redis_address))

        cli_logger.labeled_value("Local node IP", ray_params.node_ip_address)
        cli_logger.old_info(logger, "Using IP address {} for this node.",
                            ray_params.node_ip_address)

        # Check that there aren't already Redis clients with the same IP
        # address connected with this Redis instance. This raises an exception
        # if the Redis server already has clients on this node.
        check_no_existing_redis_clients(ray_params.node_ip_address,
                                        redis_client)
        ray_params.update(redis_address=redis_address)
        node = ray.node.Node(
            ray_params, head=False, shutdown_at_exit=block, spawn_reaper=block)

        cli_logger.newline()
        startup_msg = "Ray runtime started."
        cli_logger.success("-" * len(startup_msg))
        cli_logger.success(startup_msg)
        cli_logger.success("-" * len(startup_msg))
        cli_logger.newline()
        cli_logger.print("To terminate the Ray runtime, run")
        cli_logger.print(cf.bold("  ray stop"))

        cli_logger.old_info(
            logger, "\nStarted Ray on this node. If you wish to terminate the "
            "processes that have been started, run\n\n"
            "    ray stop")

    if block:
        cli_logger.newline()
        with cli_logger.group(cf.bold("--block")):
            cli_logger.print(
                "This command will now block until terminated by a signal.")
            cli_logger.print(
                "Runing subprocesses are monitored and a message will be "
                "printed if any of them terminate unexpectedly.")

        while True:
            time.sleep(1)
            deceased = node.dead_processes()
            if len(deceased) > 0:
                cli_logger.newline()
                cli_logger.error("Some Ray subprcesses exited unexpectedly:")
                cli_logger.old_error(logger,
                                     "Ray processes died unexpectedly:")

                with cli_logger.indented():
                    for process_type, process in deceased:
                        cli_logger.error(
                            "{}",
                            cf.bold(str(process_type)),
                            _tags={"exit code": str(process.returncode)})
                        cli_logger.old_error(
                            logger, "\t{} died with exit code {}".format(
                                process_type, process.returncode))

                # shutdown_at_exit will handle cleanup.
                cli_logger.newline()
                cli_logger.error("Remaining processes will be killed.")
                cli_logger.old_error(
                    logger, "Killing remaining processes and exiting...")
                sys.exit(1)


@cli.command()
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="If set, ray will send SIGKILL instead of SIGTERM.")
@add_click_options(logging_options)
def stop(force, verbose, log_new_style, log_color):
    """Stop Ray processes manually on the local machine."""

    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    # Note that raylet needs to exit before object store, otherwise
    # it cannot exit gracefully.
    is_linux = sys.platform.startswith("linux")
    processes_to_kill = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name
        # (only the first 15 charactors of the executable name on Linux);
        # if False, is to filter ps results by command with all its arguments.
        # See STANDARD FORMAT SPECIFIERS section of
        # http://man7.org/linux/man-pages/man1/ps.1.html
        # about comm and args. This can help avoid killing non-ray processes.
        # Format:
        # Keyword to filter, filter by command (True)/filter by args (False)
        ["raylet", True],
        ["plasma_store", True],
        ["gcs_server", True],
        ["monitor.py", False],
        ["redis-server", False],
        ["default_worker.py", False],  # Python worker.
        ["ray::", True],  # Python worker. TODO(mehrdadn): Fix for Windows
        ["io.ray.runtime.runner.worker.DefaultWorker", False],  # Java worker.
        ["log_monitor.py", False],
        ["reporter.py", False],
        ["dashboard.py", False],
        ["ray_process_reaper.py", False],
    ]

    process_infos = []
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            process_infos.append((proc, proc.name(), proc.cmdline()))
        except psutil.Error:
            pass

    total_found = 0
    total_stopped = 0
    for keyword, filter_by_cmd in processes_to_kill:
        if filter_by_cmd and is_linux and len(keyword) > 15:
            # getting here is an internal bug, so we do not use cli_logger
            msg = ("The filter string should not be more than {} "
                   "characters. Actual length: {}. Filter: {}").format(
                       15, len(keyword), keyword)
            raise ValueError(msg)

        found = []
        for candidate in process_infos:
            proc, proc_cmd, proc_args = candidate
            corpus = (proc_cmd
                      if filter_by_cmd else subprocess.list2cmdline(proc_args))
            if keyword in corpus:
                found.append(candidate)

        for proc, proc_cmd, proc_args in found:
            total_found += 1

            proc_string = str(subprocess.list2cmdline(proc_args))
            if verbose:
                operation = "Terminating" if force else "Killing"
                cli_logger.old_info(logger, "%s process %s: %s", operation,
                                    proc.pid, proc_string)
            try:
                if force:
                    proc.kill()
                else:
                    # TODO(mehrdadn): On Windows, this is forceful termination.
                    # We don't want CTRL_BREAK_EVENT, because that would
                    # terminate the entire process group. What to do?
                    proc.terminate()

                if force:
                    cli_logger.verbose("Killed `{}` {} ", cf.bold(proc_string),
                                       cf.gray("(via SIGKILL)"))
                else:
                    cli_logger.verbose("Send termination request to `{}` {}",
                                       cf.bold(proc_string),
                                       cf.gray("(via SIGTERM)"))

                total_stopped += 1
            except psutil.NoSuchProcess:
                cli_logger.verbose(
                    "Attempted to stop `{}`, but process was already dead.",
                    cf.bold(proc_string))
                pass
            except (psutil.Error, OSError) as ex:
                cli_logger.error("Could not terminate `{}` due to {}",
                                 cf.bold(proc_string), str(ex))
                cli_logger.old_error(logger, "Error: %s", ex)

    if total_found == 0:
        cli_logger.print("Did not find any active Ray processes.")
    else:
        if total_stopped == total_found:
            cli_logger.success("Stopped all {} Ray processes.", total_stopped)
        else:
            cli_logger.warning(
                "Stopped only {} out of {} Ray processes. "
                "Set `{}` to see more details.", total_stopped, total_found,
                cf.bold("-v"))
            cli_logger.warning("Try running the command again, or use `{}`.",
                               cf.bold("--force"))

    # TODO(maximsmol): we should probably block until the processes actually
    # all died somehow


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
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
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--dump-command-output",
    is_flag=True,
    default=False,
    help=("Print command output straight to "
          "the terminal instead of redirecting to a file."))
@click.option(
    "--use-login-shells/--use-normal-shells",
    is_flag=True,
    default=True,
    help=("Ray uses login shells (bash --login -i) to run cluster commands "
          "by default. If your workflow is compatible with normal shells, "
          "this can be disabled for a better user experience."))
@add_click_options(logging_options)
def up(cluster_config_file, min_workers, max_workers, no_restart, restart_only,
       yes, cluster_name, no_config_cache, dump_command_output,
       use_login_shells, log_new_style, log_color, verbose):
    """Create or update a Ray cluster."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    if restart_only or no_restart:
        cli_logger.doassert(restart_only != no_restart,
                            "`{}` is incompatible with `{}`.",
                            cf.bold("--restart-only"), cf.bold("--no-restart"))
        assert restart_only != no_restart, "Cannot set both 'restart_only' " \
            "and 'no_restart' at the same time!"

    if urllib.parse.urlparse(cluster_config_file).scheme in ("http", "https"):
        try:
            response = urllib.request.urlopen(cluster_config_file, timeout=5)
            content = response.read()
            file_name = cluster_config_file.split("/")[-1]
            with open(file_name, "wb") as f:
                f.write(content)
            cluster_config_file = file_name
        except urllib.error.HTTPError as e:
            cli_logger.warning("{}", str(e))
            cli_logger.warning(
                "Could not download remote cluster configuration file.")
            cli_logger.old_info(logger, "Error downloading file: ", e)
    create_or_update_cluster(cluster_config_file, min_workers, max_workers,
                             no_restart, restart_only, yes, cluster_name,
                             no_config_cache, dump_command_output,
                             use_login_shells)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--workers-only",
    is_flag=True,
    default=False,
    help="Only destroy the workers.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--keep-min-workers",
    is_flag=True,
    default=False,
    help="Retain the minimal amount of workers specified in the config.")
@add_click_options(logging_options)
def down(cluster_config_file, yes, workers_only, cluster_name,
         keep_min_workers, log_new_style, log_color, verbose):
    """Tear down a Ray cluster."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    teardown_cluster(cluster_config_file, yes, workers_only, cluster_name,
                     keep_min_workers)


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
@add_click_options(logging_options)
def monitor(cluster_config_file, lines, cluster_name, log_new_style, log_color,
            verbose):
    """Tails the autoscaler logs of a Ray cluster."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    monitor_cluster(cluster_config_file, lines, cluster_name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help="Start the cluster if needed.")
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in screen.")
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
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@add_click_options(logging_options)
def attach(cluster_config_file, start, screen, tmux, cluster_name, new,
           port_forward, log_new_style, log_color, verbose):
    """Create or attach to a SSH session to a Ray cluster."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    port_forward = [(port, port) for port in list(port_forward)]
    attach_cluster(cluster_config_file, start, screen, tmux, cluster_name, new,
                   port_forward)


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
@add_click_options(logging_options)
def rsync_down(cluster_config_file, source, target, cluster_name,
               log_new_style, log_color, verbose):
    """Download specific files from a Ray cluster."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

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
@click.option(
    "--all-nodes",
    "-A",
    is_flag=True,
    required=False,
    help="Upload to all nodes (workers and head).")
@add_click_options(logging_options)
def rsync_up(cluster_config_file, source, target, cluster_name, all_nodes,
             log_new_style, log_color, verbose):
    """Upload specific files to a Ray cluster."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    rsync(
        cluster_config_file,
        source,
        target,
        cluster_name,
        down=False,
        all_nodes=all_nodes)


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("cluster_config_file", required=True, type=str)
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
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@click.argument("script", required=True, type=str)
@click.option(
    "--args",
    required=False,
    type=str,
    help="(deprecated) Use '-- --arg1 --arg2' for script args.")
@click.argument("script_args", nargs=-1)
@add_click_options(logging_options)
def submit(cluster_config_file, screen, tmux, stop, start, cluster_name,
           port_forward, script, args, script_args, log_new_style, log_color,
           verbose):
    """Uploads and runs a script on the specified cluster.

    The script is automatically synced to the following location:

        os.path.join("~", os.path.basename(script))

    Example:
        >>> ray submit [CLUSTER.YAML] experiment.py -- --smoke-test
    """
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    set_output_redirected(False)

    cli_logger.doassert(not (screen and tmux),
                        "`{}` and `{}` are incompatible.", cf.bold("--screen"),
                        cf.bold("--tmux"))
    cli_logger.doassert(
        not (script_args and args),
        "`{0}` and `{1}` are incompatible. Use only `{1}`.\n"
        "Example: `{2}`", cf.bold("--args"), cf.bold("-- <args ...>"),
        cf.bold("ray submit script.py -- --arg=123 --flag"))

    assert not (screen and tmux), "Can specify only one of `screen` or `tmux`."
    assert not (script_args and args), "Use -- --arg1 --arg2 for script args."

    if args:
        cli_logger.warning(
            "`{}` is deprecated and will be removed in the future.",
            cf.bold("--args"))
        cli_logger.warning("Use `{}` instead. Example: `{}`.",
                           cf.bold("-- <args ...>"),
                           cf.bold("ray submit script.py -- --arg=123 --flag"))
        cli_logger.newline()
        cli_logger.old_warning(
            logger,
            "ray submit [yaml] [script.py] --args=... is deprecated and "
            "will be removed in a future version of Ray. Use "
            "`ray submit [yaml] script.py -- --arg1 --arg2` instead.")

    if start:
        create_or_update_cluster(cluster_config_file, None, None, False, False,
                                 True, cluster_name, False)
    target = os.path.basename(script)
    target = os.path.join("~", target)
    rsync(cluster_config_file, script, target, cluster_name, down=False)

    command_parts = ["python", target]
    if script_args:
        command_parts += list(script_args)
    elif args is not None:
        command_parts += [args]

    port_forward = [(port, port) for port in list(port_forward)]
    cmd = " ".join(command_parts)
    exec_cluster(
        cluster_config_file,
        cmd=cmd,
        run_env="docker",
        screen=screen,
        tmux=tmux,
        stop=stop,
        start=False,
        override_cluster_name=cluster_name,
        port_forward=port_forward)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("cmd", required=True, type=str)
@click.option(
    "--run-env",
    required=False,
    type=click.Choice(RUN_ENV_TYPES),
    default="auto",
    help="Choose whether to execute this command in a container or directly on"
    " the cluster head. Only applies when docker is configured in the YAML.")
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
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@add_click_options(logging_options)
def exec(cluster_config_file, cmd, run_env, screen, tmux, stop, start,
         cluster_name, port_forward, log_new_style, log_color, verbose):
    """Execute a command via SSH on a Ray cluster."""
    cli_logger.old_style = not log_new_style
    cli_logger.color_mode = log_color
    cli_logger.verbosity = verbose

    set_output_redirected(False)

    port_forward = [(port, port) for port in list(port_forward)]

    exec_cluster(
        cluster_config_file,
        cmd=cmd,
        run_env=run_env,
        screen=screen,
        tmux=tmux,
        stop=stop,
        start=start,
        override_cluster_name=cluster_name,
        port_forward=port_forward)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
def get_head_ip(cluster_config_file, cluster_name):
    """Return the head node IP of a Ray cluster."""
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
    """Return the list of worker IPs of a Ray cluster."""
    worker_ips = get_worker_node_ips(cluster_config_file, cluster_name)
    click.echo("\n".join(worker_ips))


@cli.command()
def stack():
    """Take a stack dump of all Python workers on the local machine."""
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
    sudo $pyspy dump --pid $pid;
    echo;
done
    """
    subprocess.call(COMMAND, shell=True)


@cli.command()
def microbenchmark():
    """Run a local Ray microbenchmark on the current machine."""
    from ray.ray_perf import main
    main()


@cli.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the redis address to connect to.")
def timeline(address):
    """Take a Chrome tracing timeline for a Ray cluster."""
    if not address:
        address = services.find_redis_address_or_die()
    logger.info("Connecting to Ray instance at {}.".format(address))
    ray.init(address=address)
    time = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(ray.utils.get_user_temp_dir(),
                            "ray-timeline-{}.json".format(time))
    ray.timeline(filename=filename)
    size = os.path.getsize(filename)
    logger.info("Trace file written to {} ({} bytes).".format(filename, size))
    logger.info(
        "You can open this with chrome://tracing in the Chrome browser.")


@cli.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
def statistics(address):
    """Get the current metrics protobuf from a Ray cluster (developer tool)."""
    if not address:
        address = services.find_redis_address_or_die()
    logger.info("Connecting to Ray instance at {}.".format(address))
    ray.init(address=address)

    import grpc
    from ray.core.generated import node_manager_pb2
    from ray.core.generated import node_manager_pb2_grpc

    for raylet in ray.nodes():
        raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                        ray.nodes()[0]["NodeManagerPort"])
        logger.info("Querying raylet {}".format(raylet_address))

        channel = grpc.insecure_channel(raylet_address)
        stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
        reply = stub.GetNodeStats(
            node_manager_pb2.GetNodeStatsRequest(include_memory_info=False),
            timeout=2.0)
        print(reply)


@cli.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
@click.option(
    "--redis_password",
    required=False,
    type=str,
    default=ray_constants.REDIS_DEFAULT_PASSWORD,
    help="Connect to ray with redis_password.")
def memory(address, redis_password):
    """Print object references held in a Ray cluster."""
    if not address:
        address = services.find_redis_address_or_die()
    logger.info("Connecting to Ray instance at {}.".format(address))
    ray.init(address=address, redis_password=redis_password)
    print(ray.internal.internal_api.memory_summary())


@cli.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
def status(address):
    """Print cluster status, including autoscaling info."""
    if not address:
        address = services.find_redis_address_or_die()
    logger.info("Connecting to Ray instance at {}.".format(address))
    ray.init(address=address)
    print(debug_status())


@cli.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
def globalgc(address):
    """Trigger Python garbage collection on all cluster workers."""
    if not address:
        address = services.find_redis_address_or_die()
    logger.info("Connecting to Ray instance at {}.".format(address))
    ray.init(address=address)
    ray.internal.internal_api.global_gc()
    print("Triggered gc.collect() on all workers.")


@cli.command()
@click.option("-v", "--verbose", is_flag=True)
@click.option(
    "--dryrun",
    is_flag=True,
    help="Identifies the wheel but does not execute the installation.")
def install_nightly(verbose, dryrun):
    """Install the latest wheels for Ray.

    This uses the same python environment as the one that Ray is currently
    installed in. Make sure that there is no Ray processes on this
    machine (ray stop) when running this command.
    """
    raydir = os.path.abspath(os.path.dirname(ray.__file__))
    all_wheels_path = os.path.join(raydir, "nightly-wheels.yaml")

    wheels = None
    if os.path.exists(all_wheels_path):
        with open(all_wheels_path) as f:
            wheels = yaml.safe_load(f)

    if not wheels:
        raise click.ClickException(
            f"Wheels not found in '{all_wheels_path}'! "
            "Please visit https://docs.ray.io/en/master/installation.html to "
            "obtain the latest wheels.")

    platform = sys.platform
    py_version = "{0}.{1}".format(*sys.version_info[:2])

    matching_wheel = None
    for target_platform, wheel_map in wheels.items():
        if verbose:
            print(f"Evaluating os={target_platform}, python={list(wheel_map)}")
        if platform.startswith(target_platform):
            if py_version in wheel_map:
                matching_wheel = wheel_map[py_version]
                break
        if verbose:
            print("Not matched.")

    if matching_wheel is None:
        raise click.ClickException(
            "Unable to identify a matching platform. "
            "Please visit https://docs.ray.io/en/master/installation.html to "
            "obtain the latest wheels.")
    if dryrun:
        print(f"Found wheel: {matching_wheel}")
    else:
        cmd = [sys.executable, "-m", "pip", "install", "-U", matching_wheel]
        print(f"Running: {' '.join(cmd)}.")
        subprocess.check_call(cmd)


def add_command_alias(command, name, hidden):
    new_command = copy.deepcopy(command)
    new_command.hidden = hidden
    cli.add_command(new_command, name=name)


cli.add_command(dashboard)
cli.add_command(start)
cli.add_command(stop)
cli.add_command(up)
add_command_alias(up, name="create_or_update", hidden=True)
cli.add_command(attach)
cli.add_command(exec)
add_command_alias(exec, name="exec_cmd", hidden=True)
add_command_alias(rsync_down, name="rsync_down", hidden=True)
add_command_alias(rsync_up, name="rsync_up", hidden=True)
cli.add_command(submit)
cli.add_command(down)
add_command_alias(down, name="teardown", hidden=True)
cli.add_command(kill_random_node)
add_command_alias(get_head_ip, name="get_head_ip", hidden=True)
cli.add_command(get_worker_ips)
cli.add_command(microbenchmark)
cli.add_command(stack)
cli.add_command(statistics)
cli.add_command(status)
cli.add_command(memory)
cli.add_command(globalgc)
cli.add_command(timeline)
cli.add_command(project_cli)
cli.add_command(session_cli)
cli.add_command(install_nightly)

try:
    from ray.serve.scripts import serve_cli
    cli.add_command(serve_cli)
except Exception as e:
    logger.debug(
        "Integrating ray serve command line tool failed with {}".format(e))


def main():
    return cli()


if __name__ == "__main__":
    main()
