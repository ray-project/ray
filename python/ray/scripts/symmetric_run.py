"""Symmetric Run for Ray.

This script will:

 - Start a Ray cluster across all nodes.
 - Run a command on the head node.
 - Stop the Ray cluster.

Currently, it also fails when there is another detected Ray instance running on the same node.
This is because Ray currently does not support the ability to kill a specific Ray instance.

Example:

    python -m ray.scripts.symmetric_run --address 127.0.0.1:6379 -- python my_script.py

    xpanes -c 4 "python -m ray.scripts.symmetric_run --address 127.0.0.1:6379 --wait-for-nnodes 4 -- python my_script.py"

"""

from typing import List, Tuple

import click
import ray
import socket
import os
import subprocess
import sys
import time

import psutil

CLUSTER_WAIT_TIMEOUT = int(os.environ.get("RAY_SYMMETRIC_RUN_CLUSTER_WAIT_TIMEOUT", 30))


def check_ray_already_started(address="auto"):
    try:
        import ray._private.services as services

        # Try auto-detecting the Ray instance.
        services.canonicalize_bootstrap_address_or_die(address)
    except ConnectionError:
        return False
    return True


def check_cluster_ready(nnodes, timeout=CLUSTER_WAIT_TIMEOUT):
    """Wait for all nodes to start.

    Raises an exception if the nodes don't start in time.
    """
    start_time = time.time()
    current_nodes = 1
    ray.init(ignore_reinit_error=True)

    while time.time() - start_time < timeout:
        time.sleep(5)
        current_nodes = len(ray.nodes())
        if current_nodes == nnodes:
            return True
        else:
            click.echo(
                f"Waiting for nodes to start... {current_nodes}/{nnodes} nodes started",
                err=True,
            )
    return False


def is_port_open(host: str, port: int, timeout: int = 5) -> bool:
    """Check if a port is open on a given host."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            result = s.connect_ex((host, port))
            return result == 0
    except (socket.error, OSError):
        return False


def check_head_node_ready(gcs_ip, gcs_port, timeout=CLUSTER_WAIT_TIMEOUT):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_port_open(gcs_ip, gcs_port):
            click.echo("Ray cluster is ready!", err=True)
            return True
        time.sleep(5)

    click.echo(
        f"Timeout: Ray cluster at {gcs_ip}:{gcs_port} not ready after {timeout}s",
        err=True,
    )
    return False


def update_ray_start_cmd(
    ray_start_cmd: List[str],
    num_cpus: int,
    num_gpus: int,
    disable_usage_stats: bool,
    resources: str,
    min_worker_port: int,
    max_worker_port: int,
    node_manager_port: int,
    object_manager_port: int,
    runtime_env_agent_port: int,
    dashboard_agent_grpc_port: int,
    dashboard_agent_listen_port: int,
    metrics_export_port: int,
):
    # Add optional parameters if provided
    if num_cpus is not None:
        ray_start_cmd.extend(["--num-cpus", str(num_cpus)])
    if num_gpus is not None:
        ray_start_cmd.extend(["--num-gpus", str(num_gpus)])
    if disable_usage_stats:
        ray_start_cmd.append("--disable-usage-stats")
    if resources != "{}":
        ray_start_cmd.extend(["--resources", resources])
    if min_worker_port:
        ray_start_cmd.extend(["--min-worker-port", str(min_worker_port)])
    if max_worker_port:
        ray_start_cmd.extend(["--max-worker-port", str(max_worker_port)])
    if node_manager_port:
        ray_start_cmd.extend(["--node-manager-port", str(node_manager_port)])
    if object_manager_port:
        ray_start_cmd.extend(["--object-manager-port", str(object_manager_port)])
    if runtime_env_agent_port:
        ray_start_cmd.extend(["--runtime-env-agent-port", str(runtime_env_agent_port)])
    if dashboard_agent_grpc_port:
        ray_start_cmd.extend(
            ["--dashboard-agent-grpc-port", str(dashboard_agent_grpc_port)]
        )
    if dashboard_agent_listen_port:
        ray_start_cmd.extend(
            ["--dashboard-agent-listen-port", str(dashboard_agent_listen_port)]
        )
    if metrics_export_port:
        ray_start_cmd.extend(["--metrics-export-port", str(metrics_export_port)])
    return ray_start_cmd


@click.command()
@click.option(
    "--address", required=True, type=str, help="The address of the Ray cluster."
)
@click.option(
    "--num-cpus",
    type=int,
    help="The number of CPUs to use for the Ray cluster. If not provided, "
    "the number of CPUs will be automatically detected.",
)
@click.option(
    "--num-gpus",
    type=int,
    help="The number of GPUs to use for the Ray cluster. If not provided, "
    "the number of GPUs will be automatically detected.",
)
@click.option(
    "--wait-for-nnodes",
    type=int,
    help="If provided, wait for this number of nodes to start.",
)
@click.option(
    "--disable-usage-stats",
    is_flag=True,
    default=False,
    help="If True, the usage stats collection will be disabled.",
)
@click.option(
    "--resources",
    required=False,
    default="{}",
    type=str,
    help="A JSON serialized dictionary mapping resource name to resource quantity.",
)
@click.option(
    "--min-worker-port",
    type=int,
    help="the lowest port number that workers will bind on. If not set, "
    "random ports will be chosen.",
)
@click.option(
    "--max-worker-port",
    type=int,
    help="the highest port number that workers will bind on. If set, "
    "'--min-worker-port' must also be set.",
)
@click.option(
    "--node-manager-port",
    type=int,
    default=0,
    help="the port to use for starting the node manager",
)
@click.option(
    "--object-manager-port",
    type=int,
    help="the port to use for starting the object manager",
)
@click.option(
    "--runtime-env-agent-port",
    type=int,
    help="The port for the runtime environment agents to listen for http on.",
)
@click.option(
    "--dashboard-agent-grpc-port",
    type=int,
    help="the port for dashboard agents to listen for grpc on.",
)
@click.option(
    "--dashboard-agent-listen-port",
    type=int,
    help="the port for dashboard agents to listen for http on.",
)
@click.option(
    "--metrics-export-port",
    type=int,
    default=None,
    help="the port to use to expose Ray metrics through a Prometheus endpoint.",
)
@click.argument("entrypoint-on-head", nargs=-1, type=click.UNPROCESSED)
def symmetric_run(
    address: str,
    wait_for_nnodes: int,
    entrypoint_on_head: Tuple[str],
    num_cpus: int,
    num_gpus: int,
    disable_usage_stats: bool,
    resources: str,
    min_worker_port: int,
    max_worker_port: int,
    node_manager_port: int,
    object_manager_port: int,
    runtime_env_agent_port: int,
    dashboard_agent_grpc_port: int,
    dashboard_agent_listen_port: int,
    metrics_export_port: int,
):
    min_nodes = 1 if wait_for_nnodes is None else wait_for_nnodes

    if check_ray_already_started():
        raise click.ClickException("Ray is already started on this node.")

    # 1. Parse address and check if we are on the head node.
    gcs_ip_port = ray._common.network_utils.parse_address(address)
    if gcs_ip_port is None:
        raise click.ClickException(f"Invalid address format: {address}")
    gcs_ip, gcs_port = gcs_ip_port

    try:
        # AF_UNSPEC allows resolving both IPv4 and IPv6
        addrinfo = socket.getaddrinfo(
            gcs_ip, gcs_port, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        resolved_gcs_ip = addrinfo[0][4][0]
    except socket.gaierror:
        raise click.ClickException(f"Could not resolve hostname: {gcs_ip}")

    my_ips = []
    for iface, addrs in psutil.net_if_addrs().items():
        for addr in addrs:
            # Look for AF_INET (IPv4) or AF_INET6 (IPv6)
            if addr.family in [
                socket.AddressFamily.AF_INET,
                socket.AddressFamily.AF_INET6,
            ]:
                my_ips.append(addr.address)

    # Add localhost ips if we are running on a single node.
    if min_nodes > 1:
        # Ban localhost ips if we are not running on a single node
        # to avoid starting N head nodes
        my_ips = [ip for ip in my_ips if ip != "127.0.0.1" and ip != "::1"]

    is_head = resolved_gcs_ip in my_ips

    result = None
    # 2. Start Ray and run commands.
    try:
        if is_head:
            # On the head node, start Ray, run the command, then stop Ray.
            click.echo("On head node. Starting Ray cluster head...", err=True)

            # Build the ray start command with all parameters
            ray_start_cmd = [
                "ray",
                "start",
                "--head",
                f"--node-ip-address={resolved_gcs_ip}",
                f"--port={gcs_port}",
            ]

            ray_start_cmd = update_ray_start_cmd(
                ray_start_cmd,
                num_cpus,
                num_gpus,
                disable_usage_stats,
                resources,
                min_worker_port,
                max_worker_port,
                node_manager_port,
                object_manager_port,
                runtime_env_agent_port,
                dashboard_agent_grpc_port,
                dashboard_agent_listen_port,
                metrics_export_port,
            )
            # Start Ray head. This runs in the background and hides output.
            subprocess.run(ray_start_cmd, check=True, capture_output=True)
            click.echo("Head node started.", err=True)
            click.echo("=======================", err=True)
            if min_nodes > 1 and not check_cluster_ready(min_nodes):
                raise click.ClickException(
                    "Timed out waiting for other nodes to start."
                )

            # Run the user command if provided.
            if entrypoint_on_head:
                click.echo(
                    f"Running command on head node: {' '.join(entrypoint_on_head)}",
                    err=True,
                )
                click.echo("=======================", err=True)
                result = subprocess.run(entrypoint_on_head)
                click.echo("=======================", err=True)
        else:
            # On a worker node, start Ray and connect to the head.
            click.echo(
                f"On worker node. Connecting to Ray cluster at {address}...", err=True
            )

            if not check_head_node_ready(gcs_ip, gcs_port):
                raise click.ClickException("Timed out waiting for head node to start.")

            # Build the ray start command for worker nodes with all parameters
            ray_start_cmd = ["ray", "start", "--address", address, "--block"]

            ray_start_cmd = update_ray_start_cmd(
                ray_start_cmd,
                num_cpus,
                num_gpus,
                disable_usage_stats,
                resources,
                min_worker_port,
                max_worker_port,
                node_manager_port,
                object_manager_port,
                runtime_env_agent_port,
                dashboard_agent_grpc_port,
                dashboard_agent_listen_port,
                metrics_export_port,
            )

            # This command will block until the Ray cluster is stopped.
            subprocess.run(ray_start_cmd, check=True)

    except subprocess.CalledProcessError as e:
        click.echo(f"Failed to start Ray: {e}", err=True)
        if e.stdout:
            click.echo(f"stdout:\n{e.stdout.decode()}", err=True)
        if e.stderr:
            click.echo(f"stderr:\n{e.stderr.decode()}", err=True)
    except KeyboardInterrupt:
        # This can be triggered by ctrl-c on the user's side.
        click.echo("Interrupted by user.", err=True)
    finally:
        # Stop Ray cluster.
        subprocess.run(["ray", "stop"])

        # Propagate the exit code of the user script.
        if result is not None and result.returncode != 0:
            click.echo(f"Command failed with return code {result.returncode}", err=True)
            sys.exit(result.returncode)


if __name__ == "__main__":
    symmetric_run()
