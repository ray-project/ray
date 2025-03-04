import copy
import asyncio
import json
import logging
import os
import platform
import signal
import subprocess
import sys
import time
import urllib
import urllib.parse
import warnings
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Set, List, Tuple
from ray.dashboard.modules.metrics import install_and_start_prometheus
from ray.util.check_open_ports import check_open_ports
import requests

import click
import colorama
import psutil
import yaml

import ray
from ray.util.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_RPC_TIMEOUT,
)
import ray._private.ray_constants as ray_constants
import ray._private.services as services
from ray._private.utils import (
    check_ray_client_dependencies_installed,
    load_class,
    parse_resources_json,
    parse_node_labels_json,
)
from ray._private.internal_api import memory_summary
from ray._private.usage import usage_lib
from ray._private.gcs_aio_client import GcsAioClient
from ray._private.gcs_utils import GcsChannel
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_service_pb2 import (
    CreateOrUpdateVirtualClusterRequest,
    GetVirtualClustersRequest,
    RemoveVirtualClusterRequest,
)
from ray.dashboard.state_aggregator import StateAPIManager
from ray.util.state.common import ListApiOptions, VirtualClusterState
from ray.util.state.state_cli import AvailableFormat, format_list_api_output
from ray.util.state.state_manager import StateDataSourceClient
from ray.autoscaler._private.cli_logger import add_click_logging_options, cf, cli_logger
from ray.autoscaler._private.commands import (
    RUN_ENV_TYPES,
    attach_cluster,
    create_or_update_cluster,
    debug_status,
    exec_cluster,
    get_cluster_dump_archive,
    get_head_node_ip,
    get_local_dump_archive,
    get_worker_node_ips,
    kill_node,
    monitor_cluster,
    rsync,
    teardown_cluster,
)
from ray.autoscaler._private.constants import RAY_PROCESSES
from ray.autoscaler._private.fake_multi_node.node_provider import FAKE_HEAD_NODE_ID
from ray.util.annotations import PublicAPI
from ray.core.generated import autoscaler_pb2


logger = logging.getLogger(__name__)


def _check_ray_version(gcs_client):
    import ray._private.usage.usage_lib as ray_usage_lib

    cluster_metadata = ray_usage_lib.get_cluster_metadata(gcs_client)
    if cluster_metadata and cluster_metadata["ray_version"] != ray.__version__:
        raise RuntimeError(
            "Ray version mismatch: cluster has Ray version "
            f'{cluster_metadata["ray_version"]} '
            f"but local Ray version is {ray.__version__}"
        )


@click.group()
@click.option(
    "--logging-level",
    required=False,
    default=ray_constants.LOGGER_LEVEL,
    type=str,
    help=ray_constants.LOGGER_LEVEL_HELP,
)
@click.option(
    "--logging-format",
    required=False,
    default=ray_constants.LOGGER_FORMAT,
    type=str,
    help=ray_constants.LOGGER_FORMAT_HELP,
)
@click.version_option()
def cli(logging_level, logging_format):
    level = logging.getLevelName(logging_level.upper())
    ray._private.ray_logging.setup_logger(level, logging_format)
    cli_logger.set_format(format_tmpl=logging_format)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@click.option(
    "--port",
    "-p",
    required=False,
    type=int,
    default=ray_constants.DEFAULT_DASHBOARD_PORT,
    help="The local port to forward to the dashboard",
)
@click.option(
    "--remote-port",
    required=False,
    type=int,
    default=ray_constants.DEFAULT_DASHBOARD_PORT,
    help="The remote port your dashboard runs on",
)
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.",
)
@PublicAPI
def dashboard(cluster_config_file, cluster_name, port, remote_port, no_config_cache):
    """Port-forward a Ray cluster's dashboard to the local machine."""
    # Sleeping in a loop is preferable to `sleep infinity` because the latter
    # only works on linux.
    # Find the first open port sequentially from `remote_port`.
    try:
        port_forward = [
            (port, remote_port),
        ]
        click.echo(
            "Attempting to establish dashboard locally at"
            " http://localhost:{}/ connected to"
            " remote port {}".format(port, remote_port)
        )
        # We want to probe with a no-op that returns quickly to avoid
        # exceptions caused by network errors.
        exec_cluster(
            cluster_config_file,
            override_cluster_name=cluster_name,
            port_forward=port_forward,
            no_config_cache=no_config_cache,
        )
        click.echo("Successfully established connection.")
    except Exception as e:
        raise click.ClickException(
            "Failed to forward dashboard from remote port {1} to local port "
            "{0}. There are a couple possibilities: \n 1. The remote port is "
            "incorrectly specified \n 2. The local port {0} is already in "
            "use.\n The exception is: {2}".format(port, remote_port, e)
        ) from None


def continue_debug_session(live_jobs: Set[str]):
    """Continue active debugging session.

    This function will connect 'ray debug' to the right debugger
    when a user is stepping between Ray tasks.
    """
    active_sessions = ray.experimental.internal_kv._internal_kv_list(
        "RAY_PDB_", namespace=ray_constants.KV_NAMESPACE_PDB
    )

    for active_session in active_sessions:
        if active_session.startswith(b"RAY_PDB_CONTINUE"):
            # Check to see that the relevant job is still alive.
            data = ray.experimental.internal_kv._internal_kv_get(
                active_session, namespace=ray_constants.KV_NAMESPACE_PDB
            )
            if json.loads(data)["job_id"] not in live_jobs:
                ray.experimental.internal_kv._internal_kv_del(
                    active_session, namespace=ray_constants.KV_NAMESPACE_PDB
                )
                continue

            print("Continuing pdb session in different process...")
            key = b"RAY_PDB_" + active_session[len("RAY_PDB_CONTINUE_") :]
            while True:
                data = ray.experimental.internal_kv._internal_kv_get(
                    key, namespace=ray_constants.KV_NAMESPACE_PDB
                )
                if data:
                    session = json.loads(data)
                    if "exit_debugger" in session or session["job_id"] not in live_jobs:
                        ray.experimental.internal_kv._internal_kv_del(
                            key, namespace=ray_constants.KV_NAMESPACE_PDB
                        )
                        return
                    host, port = session["pdb_address"].split(":")
                    ray.util.rpdb._connect_pdb_client(host, int(port))
                    ray.experimental.internal_kv._internal_kv_del(
                        key, namespace=ray_constants.KV_NAMESPACE_PDB
                    )
                    continue_debug_session(live_jobs)
                    return
                time.sleep(1.0)


def none_to_empty(s):
    if s is None:
        return ""
    return s


def format_table(table):
    """Format a table as a list of lines with aligned columns."""
    result = []
    col_width = [max(len(x) for x in col) for col in zip(*table)]
    for line in table:
        result.append(
            " | ".join("{0:{1}}".format(x, col_width[i]) for i, x in enumerate(line))
        )
    return result


@cli.command()
@click.option(
    "--address", required=False, type=str, help="Override the address to connect to."
)
@click.option(
    "-v",
    "--verbose",
    required=False,
    is_flag=True,
    help="Shows additional fields in breakpoint selection page.",
)
def debug(address: str, verbose: bool):
    """Show all active breakpoints and exceptions in the Ray debugger."""
    address = services.canonicalize_bootstrap_address_or_die(address)
    logger.info(f"Connecting to Ray instance at {address}.")
    ray.init(address=address, log_to_driver=False)
    if os.environ.get("RAY_DEBUG", "1") != "legacy":
        print(
            f"{colorama.Fore.YELLOW}NOTE: The distributed debugger "
            "https://docs.ray.io/en/latest/ray-observability"
            "/ray-distributed-debugger.html is now the default "
            "due to better interactive debugging support. If you want "
            "to keep using 'ray debug' please set RAY_DEBUG=legacy "
            f"in your cluster (e.g. via runtime environment).{colorama.Fore.RESET}"
        )
    while True:
        # Used to filter out and clean up entries from dead jobs.
        live_jobs = {
            job["JobID"] for job in ray._private.state.jobs() if not job["IsDead"]
        }
        continue_debug_session(live_jobs)

        active_sessions = ray.experimental.internal_kv._internal_kv_list(
            "RAY_PDB_", namespace=ray_constants.KV_NAMESPACE_PDB
        )
        print("Active breakpoints:")
        sessions_data = []
        for active_session in active_sessions:
            data = json.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    active_session, namespace=ray_constants.KV_NAMESPACE_PDB
                )
            )
            # Check that the relevant job is alive, else clean up the entry.
            if data["job_id"] in live_jobs:
                sessions_data.append(data)
            else:
                ray.experimental.internal_kv._internal_kv_del(
                    active_session, namespace=ray_constants.KV_NAMESPACE_PDB
                )
        sessions_data = sorted(
            sessions_data, key=lambda data: data["timestamp"], reverse=True
        )
        if verbose:
            table = [
                [
                    "index",
                    "timestamp",
                    "Ray task",
                    "filename:lineno",
                    "Task ID",
                    "Worker ID",
                    "Actor ID",
                    "Node ID",
                ]
            ]
            for i, data in enumerate(sessions_data):
                date = datetime.utcfromtimestamp(data["timestamp"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                table.append(
                    [
                        str(i),
                        date,
                        data["proctitle"],
                        data["filename"] + ":" + str(data["lineno"]),
                        data["task_id"],
                        data["worker_id"],
                        none_to_empty(data["actor_id"]),
                        data["node_id"],
                    ]
                )
        else:
            # Non verbose mode: no IDs.
            table = [["index", "timestamp", "Ray task", "filename:lineno"]]
            for i, data in enumerate(sessions_data):
                date = datetime.utcfromtimestamp(data["timestamp"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                table.append(
                    [
                        str(i),
                        date,
                        data["proctitle"],
                        data["filename"] + ":" + str(data["lineno"]),
                    ]
                )
        for i, line in enumerate(format_table(table)):
            print(line)
            if i >= 1 and not sessions_data[i - 1]["traceback"].startswith(
                "NoneType: None"
            ):
                print(sessions_data[i - 1]["traceback"])
        inp = input("Enter breakpoint index or press enter to refresh: ")
        if inp == "":
            print()
            continue
        else:
            index = int(inp)
            session = json.loads(
                ray.experimental.internal_kv._internal_kv_get(
                    active_sessions[index], namespace=ray_constants.KV_NAMESPACE_PDB
                )
            )
            host, port = session["pdb_address"].split(":")
            ray.util.rpdb._connect_pdb_client(host, int(port))


@cli.command()
@click.option(
    "--node-ip-address", required=False, type=str, help="the IP address of this node"
)
@click.option("--address", required=False, type=str, help="the address to use for Ray")
@click.option(
    "--port",
    type=int,
    required=False,
    help=f"the port of the head ray process. If not provided, defaults to "
    f"{ray_constants.DEFAULT_PORT}; if port is set to 0, we will"
    f" allocate an available port.",
)
@click.option(
    "--node-name",
    required=False,
    hidden=True,
    type=str,
    help="the user-provided identifier or name for this node. "
    "Defaults to the node's ip_address",
)
@click.option(
    "--redis-username",
    required=False,
    hidden=True,
    type=str,
    default=ray_constants.REDIS_DEFAULT_USERNAME,
    help="If provided, secure Redis ports with this username",
)
@click.option(
    "--redis-password",
    required=False,
    hidden=True,
    type=str,
    default=ray_constants.REDIS_DEFAULT_PASSWORD,
    help="If provided, secure Redis ports with this password",
)
@click.option(
    "--redis-shard-ports",
    required=False,
    hidden=True,
    type=str,
    help="the port to use for the Redis shards other than the primary Redis shard",
)
@click.option(
    "--object-manager-port",
    required=False,
    type=int,
    help="the port to use for starting the object manager",
)
@click.option(
    "--node-manager-port",
    required=False,
    type=int,
    default=0,
    help="the port to use for starting the node manager",
)
@click.option(
    "--gcs-server-port",
    required=False,
    type=int,
    help="Port number for the GCS server.",
)
@click.option(
    "--min-worker-port",
    required=False,
    type=int,
    default=10002,
    help="the lowest port number that workers will bind on. If not set, "
    "random ports will be chosen.",
)
@click.option(
    "--max-worker-port",
    required=False,
    type=int,
    default=19999,
    help="the highest port number that workers will bind on. If set, "
    "'--min-worker-port' must also be set.",
)
@click.option(
    "--worker-port-list",
    required=False,
    help="a comma-separated list of open ports for workers to bind on. "
    "Overrides '--min-worker-port' and '--max-worker-port'.",
)
@click.option(
    "--ray-client-server-port",
    required=False,
    type=int,
    default=None,
    help="the port number the ray client server binds on, default to 10001, "
    "or None if ray[client] is not installed.",
)
@click.option(
    "--memory",
    required=False,
    hidden=True,
    type=int,
    help="The amount of memory (in bytes) to make available to workers. "
    "By default, this is set to the available memory on the node.",
)
@click.option(
    "--object-store-memory",
    required=False,
    type=int,
    help="The amount of memory (in bytes) to start the object store with. "
    "By default, this is 30% (ray_constants.DEFAULT_OBJECT_STORE_MEMORY_PROPORTION) "
    "of available system memory capped by "
    "the shm size and 200G (ray_constants.DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES) "
    "but can be set higher.",
)
@click.option(
    "--redis-max-memory",
    required=False,
    hidden=True,
    type=int,
    help="The max amount of memory (in bytes) to allow redis to use. Once the "
    "limit is exceeded, redis will start LRU eviction of entries. This only "
    "applies to the sharded redis tables (task, object, and profile tables). "
    "By default this is capped at 10GB but can be set higher.",
)
@click.option(
    "--num-cpus", required=False, type=int, help="the number of CPUs on this node"
)
@click.option(
    "--num-gpus", required=False, type=int, help="the number of GPUs on this node"
)
@click.option(
    "--resources",
    required=False,
    default="{}",
    type=str,
    help="A JSON serialized dictionary mapping resource name to resource quantity."
    + (
        r"""

Windows command prompt users must ensure to double quote command line arguments. Because
JSON requires the use of double quotes you must escape these arguments as well, for
example:

    ray start --head --resources="{\"special_hardware\":1, \"custom_label\":1}"

Windows powershell users need additional escaping:

    ray start --head --resources="{\""special_hardware\"":1, \""custom_label\"":1}"
"""
        if platform.system() == "Windows"
        else ""
    ),
)
@click.option(
    "--head",
    is_flag=True,
    default=False,
    help="provide this argument for the head node",
)
@click.option(
    "--include-dashboard",
    default=None,
    type=bool,
    help="provide this argument to start the Ray dashboard GUI",
)
@click.option(
    "--dashboard-host",
    required=False,
    default=ray_constants.DEFAULT_DASHBOARD_IP,
    help="the host to bind the dashboard server to, either localhost "
    "(127.0.0.1) or 0.0.0.0 (available from all interfaces). By default, this "
    "is 127.0.0.1",
)
@click.option(
    "--dashboard-port",
    required=False,
    type=int,
    default=ray_constants.DEFAULT_DASHBOARD_PORT,
    help="the port to bind the dashboard server to--defaults to {}".format(
        ray_constants.DEFAULT_DASHBOARD_PORT
    ),
)
@click.option(
    "--dashboard-agent-listen-port",
    type=int,
    default=ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
    help="the port for dashboard agents to listen for http on.",
)
@click.option(
    "--dashboard-agent-grpc-port",
    type=int,
    default=None,
    help="the port for dashboard agents to listen for grpc on.",
)
@click.option(
    "--dashboard-grpc-port",
    type=int,
    default=None,
    help="The port for the dashboard head to listen for grpc on.",
)
@click.option(
    "--runtime-env-agent-port",
    type=int,
    default=None,
    help="The port for the runtime enviroment agents to listen for http on.",
)
@click.option(
    "--block",
    is_flag=True,
    default=False,
    help="provide this argument to block forever in this command",
)
@click.option(
    "--plasma-directory",
    required=False,
    type=str,
    help="object store directory for memory mapped files",
)
@click.option(
    "--autoscaling-config",
    required=False,
    type=str,
    help="the file that contains the autoscaling config",
)
@click.option(
    "--no-redirect-output",
    is_flag=True,
    default=False,
    help="do not redirect non-worker stdout and stderr to files",
)
@click.option(
    "--plasma-store-socket-name",
    default=None,
    help="manually specify the socket name of the plasma store",
)
@click.option(
    "--raylet-socket-name",
    default=None,
    help="manually specify the socket path of the raylet process",
)
@click.option(
    "--temp-dir",
    default=None,
    help="manually specify the root temporary dir of the Ray process, only "
    "works when --head is specified",
)
@click.option(
    "--storage",
    default=None,
    help="the persistent storage URI for the cluster. Experimental.",
)
@click.option(
    "--system-config",
    default=None,
    hidden=True,
    type=json.loads,
    help="Override system configuration defaults.",
)
@click.option(
    "--enable-object-reconstruction",
    is_flag=True,
    default=False,
    hidden=True,
    help="Specify whether object reconstruction will be used for this cluster.",
)
@click.option(
    "--metrics-export-port",
    type=int,
    default=None,
    help="the port to use to expose Ray metrics through a Prometheus endpoint.",
)
@click.option(
    "--no-monitor",
    is_flag=True,
    hidden=True,
    default=False,
    help="If True, the ray autoscaler monitor for this cluster will not be started.",
)
@click.option(
    "--tracing-startup-hook",
    type=str,
    hidden=True,
    default=None,
    help="The function that sets up tracing with a tracing provider, remote "
    "span processor, and additional instruments. See docs.ray.io/tracing.html "
    "for more info.",
)
@click.option(
    "--ray-debugger-external",
    is_flag=True,
    default=False,
    help="Make the Ray debugger available externally to the node. This is only "
    "safe to activate if the node is behind a firewall.",
)
@click.option(
    "--disable-usage-stats",
    is_flag=True,
    default=False,
    help="If True, the usage stats collection will be disabled.",
)
@click.option(
    "--labels",
    required=False,
    hidden=True,
    default="{}",
    type=str,
    help="a JSON serialized dictionary mapping label name to label value.",
)
@click.option(
    "--include-log-monitor",
    default=None,
    type=bool,
    help="If set to True or left unset, a log monitor will start monitoring "
    "the log files of all processes on this node and push their contents to GCS. "
    "Only one log monitor should be started per physical host to avoid log "
    "duplication on the driver process.",
)
@add_click_logging_options
@PublicAPI
def start(
    node_ip_address,
    address,
    port,
    node_name,
    redis_username,
    redis_password,
    redis_shard_ports,
    object_manager_port,
    node_manager_port,
    gcs_server_port,
    min_worker_port,
    max_worker_port,
    worker_port_list,
    ray_client_server_port,
    memory,
    object_store_memory,
    redis_max_memory,
    num_cpus,
    num_gpus,
    resources,
    head,
    include_dashboard,
    dashboard_host,
    dashboard_port,
    dashboard_agent_listen_port,
    dashboard_agent_grpc_port,
    dashboard_grpc_port,
    runtime_env_agent_port,
    block,
    plasma_directory,
    autoscaling_config,
    no_redirect_output,
    plasma_store_socket_name,
    raylet_socket_name,
    temp_dir,
    storage,
    system_config,
    enable_object_reconstruction,
    metrics_export_port,
    no_monitor,
    tracing_startup_hook,
    ray_debugger_external,
    disable_usage_stats,
    labels,
    include_log_monitor,
):
    """Start Ray processes manually on the local machine."""
    # TODO(hjiang): Expose physical mode interface to ray cluster start command after
    # all features implemented.

    if gcs_server_port is not None:
        cli_logger.error(
            "`{}` is deprecated and ignored. Use {} to specify "
            "GCS server port on head node.",
            cf.bold("--gcs-server-port"),
            cf.bold("--port"),
        )
    # Whether the original arguments include node_ip_address.
    include_node_ip_address = False
    if node_ip_address is not None:
        include_node_ip_address = True
        node_ip_address = services.resolve_ip_for_localhost(node_ip_address)

    resources = parse_resources_json(resources, cli_logger, cf)
    labels_dict = parse_node_labels_json(labels, cli_logger, cf)

    if plasma_store_socket_name is not None:
        warnings.warn(
            "plasma_store_socket_name is deprecated and will be removed. You are not "
            "supposed to specify this parameter as it's internal.",
            DeprecationWarning,
            stacklevel=2,
        )
    if raylet_socket_name is not None:
        warnings.warn(
            "raylet_socket_name is deprecated and will be removed. You are not "
            "supposed to specify this parameter as it's internal.",
            DeprecationWarning,
            stacklevel=2,
        )
    if temp_dir and not head:
        cli_logger.warning(
            f"`--temp-dir={temp_dir}` option will be ignored. "
            "`--head` is a required flag to use `--temp-dir`. "
            "temp_dir is only configurable from a head node. "
            "All the worker nodes will use the same temp_dir as a head node. "
        )
        temp_dir = None

    redirect_output = None if not no_redirect_output else True

    # no  client, no  port -> ok
    # no  port, has client -> default to 10001
    # has port, no  client -> value error
    # has port, has client -> ok, check port validity
    has_ray_client = check_ray_client_dependencies_installed()
    if has_ray_client and ray_client_server_port is None:
        ray_client_server_port = 10001

    ray_params = ray._private.parameter.RayParams(
        node_ip_address=node_ip_address,
        node_name=node_name if node_name else node_ip_address,
        min_worker_port=min_worker_port,
        max_worker_port=max_worker_port,
        worker_port_list=worker_port_list,
        ray_client_server_port=ray_client_server_port,
        object_manager_port=object_manager_port,
        node_manager_port=node_manager_port,
        memory=memory,
        object_store_memory=object_store_memory,
        redis_username=redis_username,
        redis_password=redis_password,
        redirect_output=redirect_output,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=resources,
        labels=labels_dict,
        autoscaling_config=autoscaling_config,
        plasma_directory=plasma_directory,
        huge_pages=False,
        plasma_store_socket_name=plasma_store_socket_name,
        raylet_socket_name=raylet_socket_name,
        temp_dir=temp_dir,
        storage=storage,
        include_dashboard=include_dashboard,
        dashboard_host=dashboard_host,
        dashboard_port=dashboard_port,
        dashboard_agent_listen_port=dashboard_agent_listen_port,
        metrics_agent_port=dashboard_agent_grpc_port,
        dashboard_grpc_port=dashboard_grpc_port,
        runtime_env_agent_port=runtime_env_agent_port,
        _system_config=system_config,
        enable_object_reconstruction=enable_object_reconstruction,
        metrics_export_port=metrics_export_port,
        no_monitor=no_monitor,
        tracing_startup_hook=tracing_startup_hook,
        ray_debugger_external=ray_debugger_external,
        enable_physical_mode=False,
        include_log_monitor=include_log_monitor,
    )

    if ray_constants.RAY_START_HOOK in os.environ:
        load_class(os.environ[ray_constants.RAY_START_HOOK])(ray_params, head)

    if head:
        # Start head node.

        if disable_usage_stats:
            usage_lib.set_usage_stats_enabled_via_env_var(False)
        usage_lib.show_usage_stats_prompt(cli=True)
        cli_logger.newline()

        if port is None:
            port = ray_constants.DEFAULT_PORT

        # Set bootstrap port.
        assert ray_params.redis_port is None
        assert ray_params.gcs_server_port is None
        ray_params.gcs_server_port = port

        if os.environ.get("RAY_FAKE_CLUSTER"):
            ray_params.env_vars = {
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": FAKE_HEAD_NODE_ID
            }

        num_redis_shards = None
        # Start Ray on the head node.
        if redis_shard_ports is not None and address is None:
            redis_shard_ports = redis_shard_ports.split(",")
            # Infer the number of Redis shards from the ports if the number is
            # not provided.
            num_redis_shards = len(redis_shard_ports)

        # This logic is deprecated and will be removed later.
        if address is not None:
            cli_logger.warning(
                "Specifying {} for external Redis address is deprecated. "
                "Please specify environment variable {}={} instead.",
                cf.bold("--address"),
                cf.bold("RAY_REDIS_ADDRESS"),
                address,
            )
            external_addresses = address.split(",")

            # We reuse primary redis as sharding when there's only one
            # instance provided.
            if len(external_addresses) == 1:
                external_addresses.append(external_addresses[0])

            ray_params.update_if_absent(external_addresses=external_addresses)
            num_redis_shards = len(external_addresses) - 1
            if redis_username == ray_constants.REDIS_DEFAULT_USERNAME:
                cli_logger.warning(
                    "`{}` should not be specified as empty string if "
                    "external Redis server(s) `{}` points to requires "
                    "username.",
                    cf.bold("--redis-username"),
                    cf.bold("--address"),
                )
            if redis_password == ray_constants.REDIS_DEFAULT_PASSWORD:
                cli_logger.warning(
                    "`{}` should not be specified as empty string if "
                    "external redis server(s) `{}` points to requires "
                    "password.",
                    cf.bold("--redis-password"),
                    cf.bold("--address"),
                )

        # Get the node IP address if one is not provided.
        ray_params.update_if_absent(node_ip_address=services.get_node_ip_address())
        cli_logger.labeled_value("Local node IP", ray_params.node_ip_address)

        # Initialize Redis settings.
        ray_params.update_if_absent(
            redis_shard_ports=redis_shard_ports,
            redis_max_memory=redis_max_memory,
            num_redis_shards=num_redis_shards,
            redis_max_clients=None,
        )

        # Fail early when starting a new cluster when one is already running
        if address is None:
            default_address = f"{ray_params.node_ip_address}:{port}"
            bootstrap_address = services.find_bootstrap_address(temp_dir)
            if (
                default_address == bootstrap_address
                and bootstrap_address in services.find_gcs_addresses()
            ):
                # The default address is already in use by a local running GCS
                # instance.
                raise ConnectionError(
                    f"Ray is trying to start at {default_address}, "
                    f"but is already running at {bootstrap_address}. "
                    "Please specify a different port using the `--port`"
                    " flag of `ray start` command."
                )

        node = ray._private.node.Node(
            ray_params, head=True, shutdown_at_exit=block, spawn_reaper=block
        )

        bootstrap_address = node.address

        # this is a noop if new-style is not set, so the old logger calls
        # are still in place
        cli_logger.newline()
        startup_msg = "Ray runtime started."
        cli_logger.success("-" * len(startup_msg))
        cli_logger.success(startup_msg)
        cli_logger.success("-" * len(startup_msg))
        cli_logger.newline()
        with cli_logger.group("Next steps"):
            dashboard_url = node.address_info["webui_url"]
            if ray_constants.ENABLE_RAY_CLUSTER:
                cli_logger.print("To add another node to this Ray cluster, run")
                # NOTE(kfstorm): Java driver rely on this line to get the address
                # of the cluster. Please be careful when updating this line.
                cli_logger.print(
                    cf.bold(" {} ray start --address='{}'"),
                    f" {ray_constants.ENABLE_RAY_CLUSTERS_ENV_VAR}=1"
                    if ray_constants.IS_WINDOWS_OR_OSX
                    else "",
                    bootstrap_address,
                )

            cli_logger.newline()
            cli_logger.print("To connect to this Ray cluster:")
            with cli_logger.indented():
                cli_logger.print("{} ray", cf.magenta("import"))
                cli_logger.print(
                    "ray{}init({})",
                    cf.magenta("."),
                    "_node_ip_address{}{}".format(
                        cf.magenta("="), cf.yellow("'" + node_ip_address + "'")
                    )
                    if include_node_ip_address
                    else "",
                )

            if dashboard_url:
                cli_logger.newline()
                cli_logger.print("To submit a Ray job using the Ray Jobs CLI:")
                cli_logger.print(
                    cf.bold(
                        "  RAY_ADDRESS='http://{}' ray job submit "
                        "--working-dir . "
                        "-- python my_script.py"
                    ),
                    dashboard_url,
                )
                cli_logger.newline()
                cli_logger.print(
                    "See https://docs.ray.io/en/latest/cluster/running-applications"
                    "/job-submission/index.html "
                )
                cli_logger.print(
                    "for more information on submitting Ray jobs to the Ray cluster."
                )

            cli_logger.newline()
            cli_logger.print("To terminate the Ray runtime, run")
            cli_logger.print(cf.bold("  ray stop"))

            cli_logger.newline()
            cli_logger.print("To view the status of the cluster, use")
            cli_logger.print("  {}".format(cf.bold("ray status")))

            if dashboard_url:
                cli_logger.newline()
                cli_logger.print("To monitor and debug Ray, view the dashboard at ")
                cli_logger.print(
                    "  {}".format(
                        cf.bold(dashboard_url),
                    )
                )

                cli_logger.newline()
                cli_logger.print(
                    cf.underlined(
                        "If connection to the dashboard fails, check your "
                        "firewall settings and "
                        "network configuration."
                    )
                )
        ray_params.gcs_address = bootstrap_address
    else:
        # Start worker node.
        if not ray_constants.ENABLE_RAY_CLUSTER:
            cli_logger.abort(
                "Multi-node Ray clusters are not supported on Windows and OSX. "
                "Restart the Ray cluster with the environment variable `{}=1` "
                "to proceed anyway.",
                cf.bold(ray_constants.ENABLE_RAY_CLUSTERS_ENV_VAR),
            )
            raise Exception(
                "Multi-node Ray clusters are not supported on Windows and OSX. "
                "Restart the Ray cluster with the environment variable "
                f"`{ray_constants.ENABLE_RAY_CLUSTERS_ENV_VAR}=1` to proceed "
                "anyway.",
            )

        # Ensure `--address` flag is specified.
        if address is None:
            cli_logger.abort(
                "`{}` is a required flag unless starting a head node with `{}`.",
                cf.bold("--address"),
                cf.bold("--head"),
            )
            raise Exception(
                "`--address` is a required flag unless starting a "
                "head node with `--head`."
            )

        # Raise error if any head-only flag are specified.
        head_only_flags = {
            "--port": port,
            "--redis-shard-ports": redis_shard_ports,
            "--include-dashboard": include_dashboard,
        }
        for flag, val in head_only_flags.items():
            if val is None:
                continue
            cli_logger.abort(
                "`{}` should only be specified when starting head node with `{}`.",
                cf.bold(flag),
                cf.bold("--head"),
            )
            raise ValueError(
                f"{flag} should only be specified when starting head node "
                "with `--head`."
            )

        # Start Ray on a non-head node.
        bootstrap_address = services.canonicalize_bootstrap_address(
            address, temp_dir=temp_dir
        )

        if bootstrap_address is None:
            cli_logger.abort(
                "Cannot canonicalize address `{}={}`.",
                cf.bold("--address"),
                cf.bold(address),
            )
            raise Exception("Cannot canonicalize address " f"`--address={address}`.")

        ray_params.gcs_address = bootstrap_address

        # Get the node IP address if one is not provided.
        ray_params.update_if_absent(
            node_ip_address=services.get_node_ip_address(bootstrap_address)
        )

        cli_logger.labeled_value("Local node IP", ray_params.node_ip_address)

        node = ray._private.node.Node(
            ray_params, head=False, shutdown_at_exit=block, spawn_reaper=block
        )
        temp_dir = node.get_temp_dir_path()

        # TODO(hjiang): Validate whether specified resource is true for physical
        # resource.

        # Ray and Python versions should probably be checked before
        # initializing Node.
        node.check_version_info()

        cli_logger.newline()
        startup_msg = "Ray runtime started."
        cli_logger.success("-" * len(startup_msg))
        cli_logger.success(startup_msg)
        cli_logger.success("-" * len(startup_msg))
        cli_logger.newline()
        cli_logger.print("To terminate the Ray runtime, run")
        cli_logger.print(cf.bold("  ray stop"))
        cli_logger.flush()

    assert ray_params.gcs_address is not None
    ray._private.utils.write_ray_address(ray_params.gcs_address, temp_dir)

    if block:
        cli_logger.newline()
        with cli_logger.group(cf.bold("--block")):
            cli_logger.print(
                "This command will now block forever until terminated by a signal."
            )
            cli_logger.print(
                "Running subprocesses are monitored and a message will be "
                "printed if any of them terminate unexpectedly. Subprocesses "
                "exit with SIGTERM will be treated as graceful, thus NOT reported."
            )
            cli_logger.flush()

        while True:
            time.sleep(1)
            deceased = node.dead_processes()

            # Report unexpected exits of subprocesses with unexpected return codes.
            # We are explicitly expecting SIGTERM because this is how `ray stop` sends
            # shutdown signal to subprocesses, i.e. log_monitor, raylet...
            # NOTE(rickyyx): We are treating 128+15 as an expected return code since
            # this is what autoscaler/_private/monitor.py does upon SIGTERM
            # handling.
            expected_return_codes = [
                0,
                signal.SIGTERM,
                -1 * signal.SIGTERM,
                128 + signal.SIGTERM,
            ]
            unexpected_deceased = [
                (process_type, process)
                for process_type, process in deceased
                if process.returncode not in expected_return_codes
            ]
            if len(unexpected_deceased) > 0:
                cli_logger.newline()
                cli_logger.error("Some Ray subprocesses exited unexpectedly:")

                with cli_logger.indented():
                    for process_type, process in unexpected_deceased:
                        cli_logger.error(
                            "{}",
                            cf.bold(str(process_type)),
                            _tags={"exit code": str(process.returncode)},
                        )

                cli_logger.newline()
                cli_logger.error("Remaining processes will be killed.")
                # explicitly kill all processes since atexit handlers
                # will not exit with errors.
                node.kill_all_processes(check_alive=False, allow_graceful=False)
                os._exit(1)
        # not-reachable


@cli.command()
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="If set, ray will send SIGKILL instead of SIGTERM.",
)
@click.option(
    "-g",
    "--grace-period",
    default=16,
    help=(
        "The time in seconds ray waits for processes to be properly terminated. "
        "If processes are not terminated within the grace period, "
        "they are forcefully terminated after the grace period. "
    ),
)
@add_click_logging_options
@PublicAPI
def stop(force: bool, grace_period: int):
    """Stop Ray processes manually on the local machine."""
    is_linux = sys.platform.startswith("linux")
    total_procs_found = 0
    total_procs_stopped = 0
    procs_not_gracefully_killed = []

    def kill_procs(
        force: bool, grace_period: int, processes_to_kill: List[str]
    ) -> Tuple[int, int, List[psutil.Process]]:
        """Find all processes from `processes_to_kill` and terminate them.

        Unless `force` is specified, it gracefully kills processes. If
        processes are not cleaned within `grace_period`, it force kill all
        remaining processes.

        Returns:
            total_procs_found: Total number of processes found from
                `processes_to_kill` is added.
            total_procs_stopped: Total number of processes gracefully
                stopped from `processes_to_kill` is added.
            procs_not_gracefully_killed: If processes are not killed
                gracefully, they are added here.
        """
        process_infos = []
        for proc in psutil.process_iter(["name", "cmdline"]):
            try:
                process_infos.append((proc, proc.name(), proc.cmdline()))
            except psutil.Error:
                pass

        stopped = []
        for keyword, filter_by_cmd in processes_to_kill:
            if filter_by_cmd and is_linux and len(keyword) > 15:
                # getting here is an internal bug, so we do not use cli_logger
                msg = (
                    "The filter string should not be more than {} "
                    "characters. Actual length: {}. Filter: {}"
                ).format(15, len(keyword), keyword)
                raise ValueError(msg)

            found = []
            for candidate in process_infos:
                proc, proc_cmd, proc_args = candidate
                corpus = (
                    proc_cmd if filter_by_cmd else subprocess.list2cmdline(proc_args)
                )
                if keyword in corpus:
                    found.append(candidate)
            for proc, proc_cmd, proc_args in found:
                proc_string = str(subprocess.list2cmdline(proc_args))
                try:
                    if force:
                        proc.kill()
                    else:
                        # TODO(mehrdadn): On Windows, this is forceful termination.
                        # We don't want CTRL_BREAK_EVENT, because that would
                        # terminate the entire process group. What to do?
                        proc.terminate()

                    if force:
                        cli_logger.verbose(
                            "Killed `{}` {} ",
                            cf.bold(proc_string),
                            cf.dimmed("(via SIGKILL)"),
                        )
                    else:
                        cli_logger.verbose(
                            "Send termination request to `{}` {}",
                            cf.bold(proc_string),
                            cf.dimmed("(via SIGTERM)"),
                        )

                    stopped.append(proc)
                except psutil.NoSuchProcess:
                    cli_logger.verbose(
                        "Attempted to stop `{}`, but process was already dead.",
                        cf.bold(proc_string),
                    )
                except (psutil.Error, OSError) as ex:
                    cli_logger.error(
                        "Could not terminate `{}` due to {}",
                        cf.bold(proc_string),
                        str(ex),
                    )

        # Wait for the processes to actually stop.
        # Dedup processes.
        stopped, alive = psutil.wait_procs(stopped, timeout=0)
        procs_to_kill = stopped + alive
        total_found = len(procs_to_kill)

        # Wait for grace period to terminate processes.
        gone_procs = set()

        def on_terminate(proc):
            gone_procs.add(proc)
            cli_logger.print(f"{len(gone_procs)}/{total_found} stopped.", end="\r")

        stopped, alive = psutil.wait_procs(
            procs_to_kill, timeout=grace_period, callback=on_terminate
        )
        total_stopped = len(stopped)

        # For processes that are not killed within the grace period,
        # we send force termination signals.
        for proc in alive:
            proc.kill()
        # Wait a little bit to make sure processes are killed forcefully.
        psutil.wait_procs(alive, timeout=2)
        return total_found, total_stopped, alive

    # Process killing procedure: we put processes into 3 buckets.
    # Bucket 1: raylet
    # Bucket 2: all other processes, e.g. dashboard, runtime env agents
    # Bucket 3: gcs_server.
    #
    # For each bucket, we send sigterm to all processes, then wait for 30s, then if
    # they are still alive, send sigkill.
    processes_to_kill = RAY_PROCESSES
    # Raylet should exit before all other processes exit.
    # Otherwise, fate-sharing agents will complain and exit.
    assert processes_to_kill[0][0] == "raylet"

    # GCS should exit after all other processes exit.
    # Otherwise, some of processes may exit with an unexpected
    # exit code which breaks ray start --block.
    assert processes_to_kill[-1][0] == "gcs_server"

    buckets = [[processes_to_kill[0]], processes_to_kill[1:-1], [processes_to_kill[-1]]]

    for bucket in buckets:
        found, stopped, alive = kill_procs(force, grace_period / len(buckets), bucket)
        total_procs_found += found
        total_procs_stopped += stopped
        procs_not_gracefully_killed.extend(alive)

    # Print the termination result.
    if total_procs_found == 0:
        cli_logger.print("Did not find any active Ray processes.")
    else:
        if total_procs_stopped == total_procs_found:
            cli_logger.success("Stopped all {} Ray processes.", total_procs_stopped)
        else:
            cli_logger.warning(
                f"Stopped only {total_procs_stopped} out of {total_procs_found} "
                f"Ray processes within the grace period {grace_period} seconds. "
                f"Set `{cf.bold('-v')}` to see more details. "
                f"Remaining processes {procs_not_gracefully_killed} "
                "will be forcefully terminated.",
            )
            cli_logger.warning(
                f"You can also use `{cf.bold('--force')}` to forcefully terminate "
                "processes or set higher `--grace-period` to wait longer time for "
                "proper termination."
            )

    # NOTE(swang): This will not reset the cluster address for a user-defined
    # temp_dir. This is fine since it will get overwritten the next time we
    # call `ray start`.
    ray._private.utils.reset_ray_address()


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--min-workers",
    required=False,
    type=int,
    help="Override the configured min worker node count for the cluster.",
)
@click.option(
    "--max-workers",
    required=False,
    type=int,
    help="Override the configured max worker node count for the cluster.",
)
@click.option(
    "--no-restart",
    is_flag=True,
    default=False,
    help=(
        "Whether to skip restarting Ray services during the update. "
        "This avoids interrupting running jobs."
    ),
)
@click.option(
    "--restart-only",
    is_flag=True,
    default=False,
    help=(
        "Whether to skip running setup commands and only restart Ray. "
        "This cannot be used with 'no-restart'."
    ),
)
@click.option(
    "--yes", "-y", is_flag=True, default=False, help="Don't ask for confirmation."
)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.",
)
@click.option(
    "--redirect-command-output",
    is_flag=True,
    default=False,
    help="Whether to redirect command output to a file.",
)
@click.option(
    "--use-login-shells/--use-normal-shells",
    is_flag=True,
    default=True,
    help=(
        "Ray uses login shells (bash --login -i) to run cluster commands "
        "by default. If your workflow is compatible with normal shells, "
        "this can be disabled for a better user experience."
    ),
)
@click.option(
    "--disable-usage-stats",
    is_flag=True,
    default=False,
    help="If True, the usage stats collection will be disabled.",
)
@add_click_logging_options
@PublicAPI
def up(
    cluster_config_file,
    min_workers,
    max_workers,
    no_restart,
    restart_only,
    yes,
    cluster_name,
    no_config_cache,
    redirect_command_output,
    use_login_shells,
    disable_usage_stats,
):
    """Create or update a Ray cluster."""
    if disable_usage_stats:
        usage_lib.set_usage_stats_enabled_via_env_var(False)

    if restart_only or no_restart:
        cli_logger.doassert(
            restart_only != no_restart,
            "`{}` is incompatible with `{}`.",
            cf.bold("--restart-only"),
            cf.bold("--no-restart"),
        )
        assert (
            restart_only != no_restart
        ), "Cannot set both 'restart_only' and 'no_restart' at the same time!"

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
            cli_logger.warning("Could not download remote cluster configuration file.")
    create_or_update_cluster(
        config_file=cluster_config_file,
        override_min_workers=min_workers,
        override_max_workers=max_workers,
        no_restart=no_restart,
        restart_only=restart_only,
        yes=yes,
        override_cluster_name=cluster_name,
        no_config_cache=no_config_cache,
        redirect_command_output=redirect_command_output,
        use_login_shells=use_login_shells,
    )


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes", "-y", is_flag=True, default=False, help="Don't ask for confirmation."
)
@click.option(
    "--workers-only", is_flag=True, default=False, help="Only destroy the workers."
)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@click.option(
    "--keep-min-workers",
    is_flag=True,
    default=False,
    help="Retain the minimal amount of workers specified in the config.",
)
@add_click_logging_options
@PublicAPI
def down(cluster_config_file, yes, workers_only, cluster_name, keep_min_workers):
    """Tear down a Ray cluster."""
    teardown_cluster(
        cluster_config_file, yes, workers_only, cluster_name, keep_min_workers
    )


@cli.command(hidden=True)
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes", "-y", is_flag=True, default=False, help="Don't ask for confirmation."
)
@click.option(
    "--hard",
    is_flag=True,
    default=False,
    help="Terminates the node via node provider (defaults to a 'soft kill'"
    " which terminates Ray but does not actually delete the instances).",
)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
def kill_random_node(cluster_config_file, yes, hard, cluster_name):
    """Kills a random Ray node. For testing purposes only."""
    click.echo(
        "Killed node with IP " + kill_node(cluster_config_file, yes, hard, cluster_name)
    )


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--lines", required=False, default=100, type=int, help="Number of lines to tail."
)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@add_click_logging_options
def monitor(cluster_config_file, lines, cluster_name):
    """Tails the autoscaler logs of a Ray cluster."""
    monitor_cluster(cluster_config_file, lines, cluster_name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--start", is_flag=True, default=False, help="Start the cluster if needed."
)
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in screen."
)
@click.option("--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.",
)
@click.option("--new", "-N", is_flag=True, help="Force creation of a new screen.")
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.",
)
@add_click_logging_options
@PublicAPI
def attach(
    cluster_config_file,
    start,
    screen,
    tmux,
    cluster_name,
    no_config_cache,
    new,
    port_forward,
):
    """Create or attach to a SSH session to a Ray cluster."""
    port_forward = [(port, port) for port in list(port_forward)]
    attach_cluster(
        cluster_config_file,
        start,
        screen,
        tmux,
        cluster_name,
        no_config_cache=no_config_cache,
        new=new,
        port_forward=port_forward,
    )


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("source", required=False, type=str)
@click.argument("target", required=False, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@add_click_logging_options
def rsync_down(cluster_config_file, source, target, cluster_name):
    """Download specific files from a Ray cluster."""
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
    help="Override the configured cluster name.",
)
@click.option(
    "--all-nodes",
    "-A",
    is_flag=True,
    required=False,
    help="Upload to all nodes (workers and head).",
)
@add_click_logging_options
def rsync_up(cluster_config_file, source, target, cluster_name, all_nodes):
    """Upload specific files to a Ray cluster."""
    if all_nodes:
        cli_logger.warning(
            "WARNING: the `all_nodes` option is deprecated and will be "
            "removed in the future. "
            "Rsync to worker nodes is not reliable since workers may be "
            "added during autoscaling. Please use the `file_mounts` "
            "feature instead for consistent file sync in autoscaling clusters"
        )

    rsync(
        cluster_config_file,
        source,
        target,
        cluster_name,
        down=False,
        all_nodes=all_nodes,
    )


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Stop the cluster after the command finishes running.",
)
@click.option(
    "--start", is_flag=True, default=False, help="Start the cluster if needed."
)
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in a screen."
)
@click.option("--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.",
)
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.",
)
@click.argument("script", required=True, type=str)
@click.option(
    "--args",
    required=False,
    type=str,
    help="(deprecated) Use '-- --arg1 --arg2' for script args.",
)
@click.argument("script_args", nargs=-1)
@click.option(
    "--disable-usage-stats",
    is_flag=True,
    default=False,
    help="If True, the usage stats collection will be disabled.",
)
@click.option(
    "--extra-screen-args",
    default=None,
    help="if screen is enabled, add the provided args to it. A useful example "
    "usage scenario is passing --extra-screen-args='-Logfile /full/path/blah_log.txt'"
    " as it redirects screen output also to a custom file",
)
@add_click_logging_options
def submit(
    cluster_config_file,
    screen,
    tmux,
    stop,
    start,
    cluster_name,
    no_config_cache,
    port_forward,
    script,
    args,
    script_args,
    disable_usage_stats,
    extra_screen_args: Optional[str] = None,
):
    """Uploads and runs a script on the specified cluster.

    The script is automatically synced to the following location:

        os.path.join("~", os.path.basename(script))

    Example:
        ray submit [CLUSTER.YAML] experiment.py -- --smoke-test
    """
    cli_logger.doassert(
        not (screen and tmux),
        "`{}` and `{}` are incompatible.",
        cf.bold("--screen"),
        cf.bold("--tmux"),
    )
    cli_logger.doassert(
        not (script_args and args),
        "`{0}` and `{1}` are incompatible. Use only `{1}`.\nExample: `{2}`",
        cf.bold("--args"),
        cf.bold("-- <args ...>"),
        cf.bold("ray submit script.py -- --arg=123 --flag"),
    )

    assert not (screen and tmux), "Can specify only one of `screen` or `tmux`."
    assert not (script_args and args), "Use -- --arg1 --arg2 for script args."

    if (extra_screen_args is not None) and (not screen):
        cli_logger.abort(
            "To use extra_screen_args, it is required to use the --screen flag"
        )

    if args:
        cli_logger.warning(
            "`{}` is deprecated and will be removed in the future.", cf.bold("--args")
        )
        cli_logger.warning(
            "Use `{}` instead. Example: `{}`.",
            cf.bold("-- <args ...>"),
            cf.bold("ray submit script.py -- --arg=123 --flag"),
        )
        cli_logger.newline()

    if start:
        if disable_usage_stats:
            usage_lib.set_usage_stats_enabled_via_env_var(False)

        create_or_update_cluster(
            config_file=cluster_config_file,
            override_min_workers=None,
            override_max_workers=None,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=cluster_name,
            no_config_cache=no_config_cache,
            redirect_command_output=False,
            use_login_shells=True,
        )
    target = os.path.basename(script)
    target = os.path.join("~", target)
    rsync(
        cluster_config_file,
        script,
        target,
        cluster_name,
        no_config_cache=no_config_cache,
        down=False,
    )

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
        no_config_cache=no_config_cache,
        port_forward=port_forward,
        extra_screen_args=extra_screen_args,
    )


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("cmd", required=True, type=str)
@click.option(
    "--run-env",
    required=False,
    type=click.Choice(RUN_ENV_TYPES),
    default="auto",
    help="Choose whether to execute this command in a container or directly on"
    " the cluster head. Only applies when docker is configured in the YAML.",
)
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Stop the cluster after the command finishes running.",
)
@click.option(
    "--start", is_flag=True, default=False, help="Start the cluster if needed."
)
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in a screen."
)
@click.option("--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.",
)
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.",
)
@click.option(
    "--disable-usage-stats",
    is_flag=True,
    default=False,
    help="If True, the usage stats collection will be disabled.",
)
@add_click_logging_options
def exec(
    cluster_config_file,
    cmd,
    run_env,
    screen,
    tmux,
    stop,
    start,
    cluster_name,
    no_config_cache,
    port_forward,
    disable_usage_stats,
):
    """Execute a command via SSH on a Ray cluster."""
    port_forward = [(port, port) for port in list(port_forward)]

    if start:
        if disable_usage_stats:
            usage_lib.set_usage_stats_enabled_via_env_var(False)

    exec_cluster(
        cluster_config_file,
        cmd=cmd,
        run_env=run_env,
        screen=screen,
        tmux=tmux,
        stop=stop,
        start=start,
        override_cluster_name=cluster_name,
        no_config_cache=no_config_cache,
        port_forward=port_forward,
        _allow_uninitialized_state=True,
    )


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.",
)
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
    help="Override the configured cluster name.",
)
def get_worker_ips(cluster_config_file, cluster_name):
    """Return the list of worker IPs of a Ray cluster."""
    worker_ips = get_worker_node_ips(cluster_config_file, cluster_name)
    click.echo("\n".join(worker_ips))


@cli.command()
def disable_usage_stats():
    """Disable usage stats collection.

    This will not affect the current running clusters
    but clusters launched in the future.
    """
    usage_lib.set_usage_stats_enabled_via_config(enabled=False)
    print(
        "Usage stats disabled for future clusters. "
        "Restart any current running clusters for this to take effect."
    )


@cli.command()
def enable_usage_stats():
    """Enable usage stats collection.

    This will not affect the current running clusters
    but clusters launched in the future.
    """
    usage_lib.set_usage_stats_enabled_via_config(enabled=True)
    print(
        "Usage stats enabled for future clusters. "
        "Restart any current running clusters for this to take effect."
    )


@cli.command()
def stack():
    """Take a stack dump of all Python workers on the local machine."""
    COMMAND = """
pyspy=`which py-spy`
if [ ! -e "$pyspy" ]; then
    echo "ERROR: Please 'pip install py-spy'" \
        "or 'pip install ray[default]' first."
    exit 1
fi
# Set IFS to iterate over lines instead of over words.
export IFS="
"
# Call sudo to prompt for password before anything has been printed.
sudo true
workers=$(
    ps aux | grep -E ' ray::|default_worker.py' | grep -v raylet | grep -v grep
)
for worker in $workers; do
    echo "Stack dump for $worker";
    pid=`echo $worker | awk '{print $2}'`;
    case "$(uname -s)" in
        Linux*) native=--native;;
        *)      native=;;
    esac
    sudo $pyspy dump --pid $pid $native;
    echo;
done
    """
    subprocess.call(COMMAND, shell=True)


@cli.command()
def microbenchmark():
    """Run a local Ray microbenchmark on the current machine."""
    from ray._private.ray_perf import main

    main()


@cli.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the Ray address to connect to.",
)
def timeline(address):
    """Take a Chrome tracing timeline for a Ray cluster."""
    address = services.canonicalize_bootstrap_address_or_die(address)
    logger.info(f"Connecting to Ray instance at {address}.")
    ray.init(address=address)
    time = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(
        ray._private.utils.get_user_temp_dir(), f"ray-timeline-{time}.json"
    )
    ray.timeline(filename=filename)
    size = os.path.getsize(filename)
    logger.info(f"Trace file written to {filename} ({size} bytes).")
    logger.info("You can open this with chrome://tracing in the Chrome browser.")


@cli.command()
@click.option(
    "--address", required=False, type=str, help="Override the address to connect to."
)
@click.option(
    "--group-by",
    type=click.Choice(["NODE_ADDRESS", "STACK_TRACE"]),
    default="NODE_ADDRESS",
    help="Group object references by a GroupByType \
(e.g. NODE_ADDRESS or STACK_TRACE).",
)
@click.option(
    "--sort-by",
    type=click.Choice(["PID", "OBJECT_SIZE", "REFERENCE_TYPE"]),
    default="OBJECT_SIZE",
    help="Sort object references in ascending order by a SortingType \
(e.g. PID, OBJECT_SIZE, or REFERENCE_TYPE).",
)
@click.option(
    "--units",
    type=click.Choice(["B", "KB", "MB", "GB"]),
    default="B",
    help="Specify unit metrics for displaying object sizes \
(e.g. B, KB, MB, GB).",
)
@click.option(
    "--no-format",
    is_flag=True,
    type=bool,
    default=True,
    help="Display unformatted results. Defaults to true when \
terminal width is less than 137 characters.",
)
@click.option(
    "--stats-only", is_flag=True, default=False, help="Display plasma store stats only."
)
@click.option(
    "--num-entries",
    "--n",
    type=int,
    default=None,
    help="Specify number of sorted entries per group.",
)
def memory(
    address,
    group_by,
    sort_by,
    units,
    no_format,
    stats_only,
    num_entries,
):
    """Print object references held in a Ray cluster."""
    address = services.canonicalize_bootstrap_address_or_die(address)
    gcs_client = ray._raylet.GcsClient(address=address)
    _check_ray_version(gcs_client)
    time = datetime.now()
    header = "=" * 8 + f" Object references status: {time} " + "=" * 8
    mem_stats = memory_summary(
        address,
        group_by,
        sort_by,
        units,
        no_format,
        stats_only,
        num_entries,
    )
    print(f"{header}\n{mem_stats}")


@cli.command()
@click.option(
    "--address", required=False, type=str, help="Override the address to connect to."
)
@click.option(
    "-v",
    "--verbose",
    required=False,
    is_flag=True,
    hidden=True,
    help="Experimental: Display additional debuggging information.",
)
@PublicAPI
def status(address: str, verbose: bool):
    """Print cluster status, including autoscaling info."""
    address = services.canonicalize_bootstrap_address_or_die(address)
    gcs_client = ray._raylet.GcsClient(address=address)
    _check_ray_version(gcs_client)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    status = gcs_client.internal_kv_get(ray_constants.DEBUG_AUTOSCALING_STATUS.encode())
    error = gcs_client.internal_kv_get(ray_constants.DEBUG_AUTOSCALING_ERROR.encode())
    print(debug_status(status, error, verbose=verbose, address=address))


@cli.command(hidden=True)
@click.option(
    "--stream",
    "-S",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="If True, will stream the binary archive contents to stdout",
)
@click.option(
    "--output", "-o", required=False, type=str, default=None, help="Output file."
)
@click.option(
    "--logs/--no-logs",
    is_flag=True,
    default=True,
    help="Collect logs from ray session dir",
)
@click.option(
    "--debug-state/--no-debug-state",
    is_flag=True,
    default=True,
    help="Collect debug_state.txt from ray session dir",
)
@click.option(
    "--pip/--no-pip", is_flag=True, default=True, help="Collect installed pip packages"
)
@click.option(
    "--processes/--no-processes",
    is_flag=True,
    default=True,
    help="Collect info on running processes",
)
@click.option(
    "--processes-verbose/--no-processes-verbose",
    is_flag=True,
    default=True,
    help="Increase process information verbosity",
)
@click.option(
    "--tempfile",
    "-T",
    required=False,
    type=str,
    default=None,
    help="Temporary file to use",
)
def local_dump(
    stream: bool = False,
    output: Optional[str] = None,
    logs: bool = True,
    debug_state: bool = True,
    pip: bool = True,
    processes: bool = True,
    processes_verbose: bool = False,
    tempfile: Optional[str] = None,
):
    """Collect local data and package into an archive.

    Usage:

        ray local-dump [--stream/--output file]

    This script is called on remote nodes to fetch their data.
    """
    # This may stream data to stdout, so no printing here
    get_local_dump_archive(
        stream=stream,
        output=output,
        logs=logs,
        debug_state=debug_state,
        pip=pip,
        processes=processes,
        processes_verbose=processes_verbose,
        tempfile=tempfile,
    )


@cli.command()
@click.argument("cluster_config_file", required=False, type=str)
@click.option(
    "--host",
    "-h",
    required=False,
    type=str,
    help="Single or list of hosts, separated by comma.",
)
@click.option(
    "--ssh-user",
    "-U",
    required=False,
    type=str,
    default=None,
    help="Username of the SSH user.",
)
@click.option(
    "--ssh-key",
    "-K",
    required=False,
    type=str,
    default=None,
    help="Path to the SSH key file.",
)
@click.option(
    "--docker",
    "-d",
    required=False,
    type=str,
    default=None,
    help="Name of the docker container, if applicable.",
)
@click.option(
    "--local",
    "-L",
    required=False,
    type=bool,
    is_flag=True,
    default=None,
    help="Also include information about the local node.",
)
@click.option(
    "--output", "-o", required=False, type=str, default=None, help="Output file."
)
@click.option(
    "--logs/--no-logs",
    is_flag=True,
    default=True,
    help="Collect logs from ray session dir",
)
@click.option(
    "--debug-state/--no-debug-state",
    is_flag=True,
    default=True,
    help="Collect debug_state.txt from ray log dir",
)
@click.option(
    "--pip/--no-pip", is_flag=True, default=True, help="Collect installed pip packages"
)
@click.option(
    "--processes/--no-processes",
    is_flag=True,
    default=True,
    help="Collect info on running processes",
)
@click.option(
    "--processes-verbose/--no-processes-verbose",
    is_flag=True,
    default=True,
    help="Increase process information verbosity",
)
@click.option(
    "--tempfile",
    "-T",
    required=False,
    type=str,
    default=None,
    help="Temporary file to use",
)
def cluster_dump(
    cluster_config_file: Optional[str] = None,
    host: Optional[str] = None,
    ssh_user: Optional[str] = None,
    ssh_key: Optional[str] = None,
    docker: Optional[str] = None,
    local: Optional[bool] = None,
    output: Optional[str] = None,
    logs: bool = True,
    debug_state: bool = True,
    pip: bool = True,
    processes: bool = True,
    processes_verbose: bool = False,
    tempfile: Optional[str] = None,
):
    """Get log data from one or more nodes.

    Best used with Ray cluster configs:

        ray cluster-dump [cluster.yaml]

    Include the --local flag to also collect and include data from the
    local node.

    Missing fields will be tried to be auto-filled.

    You can also manually specify a list of hosts using the
    ``--host <host1,host2,...>`` parameter.
    """
    archive_path = get_cluster_dump_archive(
        cluster_config_file=cluster_config_file,
        host=host,
        ssh_user=ssh_user,
        ssh_key=ssh_key,
        docker=docker,
        local=local,
        output=output,
        logs=logs,
        debug_state=debug_state,
        pip=pip,
        processes=processes,
        processes_verbose=processes_verbose,
        tempfile=tempfile,
    )
    if archive_path:
        click.echo(f"Created archive: {archive_path}")
    else:
        click.echo("Could not create archive.")


@cli.command(hidden=True)
@click.option(
    "--address", required=False, type=str, help="Override the address to connect to."
)
def global_gc(address):
    """Trigger Python garbage collection on all cluster workers."""
    ray.init(address=address)
    ray._private.internal_api.global_gc()
    print("Triggered gc.collect() on all workers.")


@cli.command(hidden=True)
@click.option(
    "--address", required=False, type=str, help="Override the address to connect to."
)
@click.option(
    "--node-id",
    required=True,
    type=str,
    help="Hex ID of the worker node to be drained.",
)
@click.option(
    "--reason",
    required=True,
    type=click.Choice(
        [
            item[0]
            for item in autoscaler_pb2.DrainNodeReason.items()
            if item[1] != autoscaler_pb2.DRAIN_NODE_REASON_UNSPECIFIED
        ]
    ),
    help="The reason why the node will be drained.",
)
@click.option(
    "--reason-message",
    required=True,
    type=str,
    help="The detailed drain reason message.",
)
@click.option(
    "--deadline-remaining-seconds",
    required=False,
    type=int,
    default=None,
    help="Inform GCS that the node to be drained will be force killed "
    "after this many of seconds. "
    "Default is None which means there is no deadline. "
    "Note: This command doesn't actually force kill the node after the deadline, "
    "it's the caller's responsibility to do that.",
)
def drain_node(
    address: str,
    node_id: str,
    reason: str,
    reason_message: str,
    deadline_remaining_seconds: int,
):
    """
    This is NOT a public API.

    Manually drain a worker node.
    """
    deadline_timestamp_ms = 0
    if deadline_remaining_seconds is not None:
        if deadline_remaining_seconds < 0:
            raise click.BadParameter(
                "--deadline-remaining-seconds cannot be negative, "
                f"got {deadline_remaining_seconds}"
            )
        deadline_timestamp_ms = (time.time_ns() // 1000000) + (
            deadline_remaining_seconds * 1000
        )

    if ray.NodeID.from_hex(node_id) == ray.NodeID.nil():
        raise click.BadParameter(f"Invalid hex ID of a Ray node, got {node_id}")

    address = services.canonicalize_bootstrap_address_or_die(address)

    gcs_client = ray._raylet.GcsClient(address=address)
    _check_ray_version(gcs_client)
    is_accepted, rejection_error_message = gcs_client.drain_node(
        node_id,
        autoscaler_pb2.DrainNodeReason.Value(reason),
        reason_message,
        deadline_timestamp_ms,
    )

    if not is_accepted:
        raise click.ClickException(
            f"The drain request is not accepted: {rejection_error_message}"
        )


@cli.command(name="kuberay-autoscaler", hidden=True)
@click.option(
    "--cluster-name",
    required=True,
    type=str,
    help="The name of the Ray Cluster.\n"
    "Should coincide with the `metadata.name` of the RayCluster CR.",
)
@click.option(
    "--cluster-namespace",
    required=True,
    type=str,
    help="The Kubernetes namespace the Ray Cluster lives in.\n"
    "Should coincide with the `metadata.namespace` of the RayCluster CR.",
)
def kuberay_autoscaler(cluster_name: str, cluster_namespace: str) -> None:
    """Runs the autoscaler for a Ray cluster managed by the KubeRay operator.

    `ray kuberay-autoscaler` is meant to be used as an entry point in
        KubeRay cluster configs.
    `ray kuberay-autoscaler` is NOT a public CLI.
    """
    # Delay import to avoid introducing Ray core dependency on the Python Kubernetes
    # client.
    from ray.autoscaler._private.kuberay.run_autoscaler import run_kuberay_autoscaler

    run_kuberay_autoscaler(cluster_name, cluster_namespace)


@cli.command(name="health-check", hidden=True)
@click.option(
    "--address", required=False, type=str, help="Override the address to connect to."
)
@click.option(
    "--component",
    required=False,
    type=str,
    help="Health check for a specific component. Currently supports: "
    "[ray_client_server]",
)
@click.option(
    "--skip-version-check",
    is_flag=True,
    default=False,
    help="Skip comparison of GCS version with local Ray version.",
)
def healthcheck(address, component, skip_version_check):
    """
    This is NOT a public API.

    Health check a Ray or a specific component. Exit code 0 is healthy.
    """

    address = services.canonicalize_bootstrap_address_or_die(address)
    gcs_client = ray._raylet.GcsClient(address=address)
    if not skip_version_check:
        _check_ray_version(gcs_client)

    if not component:
        sys.exit(0)

    report_str = gcs_client.internal_kv_get(
        component.encode(), namespace=ray_constants.KV_NAMESPACE_HEALTHCHECK
    )
    if not report_str:
        # Status was never updated
        sys.exit(1)

    report = json.loads(report_str)

    # TODO (Alex): We probably shouldn't rely on time here, but cloud providers
    # have very well synchronized NTP servers, so this should be fine in
    # practice.
    cur_time = time.time()
    report_time = float(report["time"])

    # If the status is too old, the service has probably already died.
    delta = cur_time - report_time
    time_ok = delta < ray._private.ray_constants.HEALTHCHECK_EXPIRATION_S

    if time_ok:
        sys.exit(0)
    else:
        sys.exit(1)


@cli.command()
@click.option("-v", "--verbose", is_flag=True)
@click.option(
    "--dryrun",
    is_flag=True,
    help="Identifies the wheel but does not execute the installation.",
)
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
            "obtain the latest wheels."
        )

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
            "obtain the latest wheels."
        )
    if dryrun:
        print(f"Found wheel: {matching_wheel}")
    else:
        cmd = [sys.executable, "-m", "pip", "install", "-U", matching_wheel]
        print(f"Running: {' '.join(cmd)}.")
        subprocess.check_call(cmd)


@cli.command()
@click.option(
    "--show-library-path",
    "-show",
    required=False,
    is_flag=True,
    help="Show the cpp include path and library path, if provided.",
)
@click.option(
    "--generate-bazel-project-template-to",
    "-gen",
    required=False,
    type=str,
    help="The directory to generate the bazel project template to, if provided.",
)
@add_click_logging_options
def cpp(show_library_path, generate_bazel_project_template_to):
    """Show the cpp library path and generate the bazel project template."""
    if sys.platform == "win32":
        cli_logger.error("Ray C++ API is not supported on Windows currently.")
        sys.exit(1)
    if not show_library_path and not generate_bazel_project_template_to:
        raise ValueError(
            "Please input at least one option of '--show-library-path'"
            " and '--generate-bazel-project-template-to'."
        )
    raydir = os.path.abspath(os.path.dirname(ray.__file__))
    cpp_dir = os.path.join(raydir, "cpp")
    cpp_templete_dir = os.path.join(cpp_dir, "example")
    include_dir = os.path.join(cpp_dir, "include")
    lib_dir = os.path.join(cpp_dir, "lib")
    if not os.path.isdir(cpp_dir):
        raise ValueError('Please install ray with C++ API by "pip install ray[cpp]".')
    if show_library_path:
        cli_logger.print("Ray C++ include path {} ", cf.bold(f"{include_dir}"))
        cli_logger.print("Ray C++ library path {} ", cf.bold(f"{lib_dir}"))
    if generate_bazel_project_template_to:
        # copytree expects that the dst dir doesn't exist
        # so we manually delete it if it exists.
        if os.path.exists(generate_bazel_project_template_to):
            shutil.rmtree(generate_bazel_project_template_to)
        shutil.copytree(cpp_templete_dir, generate_bazel_project_template_to)
        out_include_dir = os.path.join(
            generate_bazel_project_template_to, "thirdparty/include"
        )
        if os.path.exists(out_include_dir):
            shutil.rmtree(out_include_dir)
        shutil.copytree(include_dir, out_include_dir)
        out_lib_dir = os.path.join(generate_bazel_project_template_to, "thirdparty/lib")
        if os.path.exists(out_lib_dir):
            shutil.rmtree(out_lib_dir)
        shutil.copytree(lib_dir, out_lib_dir)

        cli_logger.print(
            "Project template generated to {}",
            cf.bold(f"{os.path.abspath(generate_bazel_project_template_to)}"),
        )
        cli_logger.print("To build and run this template, run")
        cli_logger.print(
            cf.bold(
                f"    cd {os.path.abspath(generate_bazel_project_template_to)}"
                " && bash run.sh"
            )
        )


@click.group(name="metrics")
def metrics_group():
    pass


@metrics_group.command(name="launch-prometheus")
def launch_prometheus():
    install_and_start_prometheus.main()


@metrics_group.command(name="shutdown-prometheus")
def shutdown_prometheus():
    try:
        requests.post("http://localhost:9090/-/quit")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


def add_command_alias(command, name, hidden):
    new_command = copy.deepcopy(command)
    new_command.hidden = hidden
    cli.add_command(new_command, name=name)


def _get_virtual_cluster_stub(address: Optional[str] = None):
    address = services.canonicalize_bootstrap_address_or_die(address)
    channel = GcsChannel(address, aio=False)
    channel.connect()
    return gcs_service_pb2_grpc.VirtualClusterInfoGcsServiceStub(channel.channel())


@click.group("vcluster")
def vclusters_cli_group():
    """Create, update or remove virtual clusters."""
    pass


@vclusters_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@click.option(
    "--id",
    type=str,
    required=True,
    help="Virtual Cluster ID to create.",
)
@click.option(
    "--divisible",
    is_flag=True,
    default=False,
    help="Whether the virtual cluster is divisible.",
)
@click.option(
    "--replica-sets",
    type=str,
    default=None,
    required=True,
    help="JSON-serialized dictionary of replica sets.",
)
@add_click_logging_options
def create(
    address: Optional[str],
    id: str,
    divisible: bool,
    replica_sets: Optional[str],
):
    """Create a new virtual cluster."""
    stub = _get_virtual_cluster_stub(address)
    reply = stub.GetVirtualClusters(GetVirtualClustersRequest(virtual_cluster_id=id))
    if reply.status.code != 0:
        cli_logger.error(
            f"Failed to create virtual cluster '{id}': {reply.status.message}"
        )
        sys.exit(1)

    if len(reply.virtual_cluster_data_list) > 0:
        cli_logger.error(f"Failed to create virtual cluster '{id}': already exists")
        sys.exit(1)

    if replica_sets is not None:
        replica_sets = json.loads(replica_sets)

    request = CreateOrUpdateVirtualClusterRequest(
        virtual_cluster_id=id,
        divisible=divisible,
        replica_sets=replica_sets,
    )

    reply = stub.CreateOrUpdateVirtualCluster(request)

    if reply.status.code == 0:
        cli_logger.success(f"Virtual cluster '{id}' created successfully")
    else:
        cli_logger.error(
            f"Failed to create virtual cluster '{id}': {reply.status.message}"
        )
        sys.exit(1)


@vclusters_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@click.option(
    "--id",
    type=str,
    required=True,
    help="Virtual Cluster ID to create.",
)
@click.option(
    "--divisible",
    is_flag=True,
    default=False,
    help="Whether the virtual cluster is divisible.",
)
@click.option(
    "--replica-sets",
    type=str,
    default=None,
    required=True,
    help="JSON-serialized dictionary of replica sets.",
)
@click.option(
    "--revision",
    type=int,
    required=True,
    help="Revision number for the virtual cluster.",
)
@add_click_logging_options
def update(
    address: Optional[str],
    id: str,
    divisible: bool,
    replica_sets: Optional[str],
    revision: int,
):
    """Update an existing virtual cluster."""
    stub = _get_virtual_cluster_stub(address)
    if replica_sets is not None:
        replica_sets = json.loads(replica_sets)

    request = CreateOrUpdateVirtualClusterRequest(
        virtual_cluster_id=id,
        divisible=divisible,
        replica_sets=replica_sets,
        revision=revision,
    )

    reply = stub.CreateOrUpdateVirtualCluster(request)

    if reply.status.code == 0:
        cli_logger.success(f"Virtual cluster '{id}' updated successfully")
    else:
        cli_logger.error(
            f"Failed to update virtual cluster '{id}': {reply.status.message}"
        )
        sys.exit(1)


@vclusters_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@click.argument("virtual-cluster-id", type=str)
@add_click_logging_options
def remove(address: Optional[str], virtual_cluster_id: str):
    """Remove a virtual cluster."""
    stub = _get_virtual_cluster_stub(address)
    request = RemoveVirtualClusterRequest(virtual_cluster_id=virtual_cluster_id)

    reply = stub.RemoveVirtualCluster(request)

    if reply.status.code == 0:
        cli_logger.success(
            f"Virtual cluster '{virtual_cluster_id}' removed successfully"
        )
    else:
        cli_logger.error(
            f"Failed to remove virtual cluster '{virtual_cluster_id}'"
            f": {reply.status.message}"
        )
        sys.exit(1)


def _get_available_formats() -> List[str]:
    """Return the available formats in a list of string"""
    return [format_enum.value for format_enum in AvailableFormat]


@vclusters_cli_group.command()
@click.option(
    "--address",
    type=str,
    default=None,
    required=False,
    help=(
        "Address of the Ray cluster to connect to. Can also be specified "
        "using the RAY_ADDRESS environment variable."
    ),
)
@click.option(
    "--format", default="default", type=click.Choice(_get_available_formats())
)
@click.option(
    "-f",
    "--filter",
    help=(
        "A key, predicate, and value to filter the result. "
        "E.g., --filter 'key=value' or --filter 'key!=value'. "
        "You can specify multiple --filter options. In this case all predicates "
        "are concatenated as AND. For example, --filter key=value --filter key2=value "
        "means (key==val) AND (key2==val2), "
        "String filter values are case-insensitive."
    ),
    multiple=True,
)
@click.option(
    "--limit",
    default=DEFAULT_LIMIT,
    type=int,
    help=("Maximum number of entries to return. 100 by default."),
)
@click.option(
    "--detail",
    help=(
        "If the flag is set, the output will contain data in more details. "
        "Note that the API could query more sources "
        "to obtain information in a greater detail."
    ),
    is_flag=True,
    default=False,
)
@click.option(
    "--timeout",
    type=int,
    default=DEFAULT_RPC_TIMEOUT,
    required=False,
    help="Timeout for the request.",
)
@add_click_logging_options
def list(
    address: Optional[str],
    timeout: Optional[int],
    limit: Optional[int],
    filter: Optional[str],
    detail: Optional[bool],
    format: Optional[str],
):
    """Lists all virtual clusters and their information.

    Example:
        `ray vcluster list`
    """

    async def run(address):
        address = services.canonicalize_bootstrap_address_or_die(address)
        gcs_aio_client = GcsAioClient(address=address)
        gcs_channel = GcsChannel(gcs_address=address, aio=True)
        gcs_channel.connect()
        state_api_data_source_client = StateDataSourceClient(
            gcs_channel.channel(), gcs_aio_client
        )
        state_api_manager = StateAPIManager(
            state_api_data_source_client,
            thread_pool_executor=ThreadPoolExecutor(
                thread_name_prefix="state_api_test_utils"
            ),
        )
        data = await state_api_manager.list_vclusters(
            option=ListApiOptions(
                timeout=timeout,
                limit=limit,
                filters=filter,
                detail=detail,
            )
        )
        await state_api_data_source_client._client_session.close()
        fmt = AvailableFormat(format)
        if detail and fmt == AvailableFormat.DEFAULT:
            fmt = AvailableFormat.TABLE

        result = [VirtualClusterState(**vcluster) for vcluster in data.result]

        print(
            format_list_api_output(
                state_data=result,
                schema=VirtualClusterState,
                format=fmt,
                detail=detail,
            )
        )

    asyncio.run(run(address))


cli.add_command(dashboard)
cli.add_command(debug)
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
cli.add_command(status)
cli.add_command(memory)
cli.add_command(local_dump)
cli.add_command(cluster_dump)
cli.add_command(global_gc)
cli.add_command(timeline)
cli.add_command(install_nightly)
cli.add_command(cpp)
cli.add_command(disable_usage_stats)
cli.add_command(enable_usage_stats)
cli.add_command(metrics_group)
cli.add_command(drain_node)
cli.add_command(check_open_ports)
cli.add_command(vclusters_cli_group)

try:
    from ray.util.state.state_cli import (
        ray_get,
        ray_list,
        logs_state_cli_group,
        summary_state_cli_group,
    )

    cli.add_command(ray_list, name="list")
    cli.add_command(ray_get, name="get")
    add_command_alias(summary_state_cli_group, name="summary", hidden=False)
    add_command_alias(logs_state_cli_group, name="logs", hidden=False)
except ImportError as e:
    logger.debug(f"Integrating ray state command line tool failed: {e}")


try:
    from ray.dashboard.modules.job.cli import job_cli_group

    add_command_alias(job_cli_group, name="job", hidden=False)
except Exception as e:
    logger.debug(f"Integrating ray jobs command line tool failed with {e}")


try:
    from ray.serve.scripts import serve_cli

    cli.add_command(serve_cli)
except Exception as e:
    logger.debug(f"Integrating ray serve command line tool failed with {e}")


def main():
    return cli()


if __name__ == "__main__":
    main()
