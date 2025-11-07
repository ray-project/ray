"""Symmetric Run for Ray."""

import socket
import subprocess
import sys
import time
from typing import List

import click

import ray
from ray._private.ray_constants import env_integer
from ray._raylet import GcsClient

import psutil

CLUSTER_WAIT_TIMEOUT = env_integer("RAY_SYMMETRIC_RUN_CLUSTER_WAIT_TIMEOUT", 30)


def check_ray_already_started() -> bool:
    import ray._private.services as services

    # Try auto-detecting the Ray instance.
    running_gcs_addresses = services.find_gcs_addresses()
    return len(running_gcs_addresses) > 0


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
                f"Waiting for nodes to start... {current_nodes}/{nnodes} nodes started"
            )
    return False


def check_head_node_ready(address: str, timeout=CLUSTER_WAIT_TIMEOUT):
    start_time = time.time()
    gcs_client = GcsClient(address=address)
    while time.time() - start_time < timeout:
        if gcs_client.check_alive([], timeout=1):
            click.echo("Ray cluster is ready!")
            return True
        time.sleep(5)
    return False


def curate_and_validate_ray_start_args(run_and_start_args: List[str]) -> List[str]:
    # Reparse the arguments to remove symmetric_run arguments.
    ctx = symmetric_run.make_context("_", run_and_start_args, resilient_parsing=True)
    cleaned_args = list(ctx.params["ray_args_and_entrypoint"])

    for arg in cleaned_args:
        if arg == "--head":
            raise click.ClickException("Cannot use --head option in symmetric_run.")
        if arg == "--node-ip-address":
            raise click.ClickException(
                "Cannot use --node-ip-address option in symmetric_run."
            )
        if arg == "--port":
            raise click.ClickException("Cannot use --port option in symmetric_run.")
        if arg == "--block":
            raise click.ClickException("Cannot use --block option in symmetric_run.")

    return cleaned_args


@click.command(
    name="symmetric_run",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="""Command to start Ray across all nodes and execute an entrypoint command.

USAGE:

    ray symmetric-run --address ADDRESS
[--min-nodes NUM_NODES] [RAY_START_OPTIONS] -- [ENTRYPOINT_COMMAND]

DESCRIPTION:

    This command (1) starts a Ray cluster across all nodes,
(2) runs a command on the head node, and (3) stops the Ray cluster.

    The '--' separator is required to distinguish between Ray start arguments
and the entrypoint command. The --min-nodes option is optional and
can be used to wait for a specific number of nodes to start.

EXAMPLES:

    # Start Ray with default settings and run a Python script

    ray symmetric-run --address 127.0.0.1:6379 -- python my_script.py

    # Start Ray with specific head node and run a command

    ray symmetric-run --address 127.0.0.1:6379 --min-nodes 4 -- python train_model.py --epochs=100

    # Start Ray and run a multi-word command

    ray symmetric-run --address 127.0.0.1:6379 --min-nodes 4 --num-cpus=4 -- python -m my_module --config=prod

RAY START OPTIONS:

    Most ray start command options are supported. Arguments that are not
supported are: --head, --node-ip-address, --port, --block.

SEPARATOR REQUIREMENT:

    The '--' separator is mandatory and must appear between Ray start
    arguments and the entrypoint command. This ensures clear separation
    between the two sets of arguments.
""",
)
@click.option(
    "--address", required=True, type=str, help="The address of the Ray cluster."
)
@click.option(
    "--min-nodes",
    type=int,
    help="If provided, wait for this number of nodes to start.",
)
@click.argument("ray_args_and_entrypoint", nargs=-1, type=click.UNPROCESSED)
def symmetric_run(address, min_nodes, ray_args_and_entrypoint):
    all_args = sys.argv[1:]

    if all_args and all_args[0] == "symmetric-run":
        all_args = all_args[1:]

    try:
        separator = all_args.index("--")
    except ValueError:
        raise click.ClickException(
            "No separator '--' found in arguments. Please use '--' to "
            "separate Ray start arguments and the entrypoint command."
        )

    run_and_start_args, entrypoint_on_head = (
        all_args[:separator],
        all_args[separator + 1 :],
    )

    ray_start_args = curate_and_validate_ray_start_args(run_and_start_args)

    min_nodes = 1 if min_nodes is None else min_nodes

    if not entrypoint_on_head:
        raise click.ClickException("No entrypoint command provided.")

    if check_ray_already_started():
        raise click.ClickException("Ray is already started on this node.")

    # 1. Parse address and check if we are on the head node.
    gcs_host_port = ray._common.network_utils.parse_address(address)
    if gcs_host_port is None:
        raise click.ClickException(
            f"Invalid address format: {address}, should be `host:port`"
        )
    gcs_host, gcs_port = gcs_host_port

    try:
        # AF_UNSPEC allows resolving both IPv4 and IPv6
        addrinfo = socket.getaddrinfo(
            gcs_host, gcs_port, socket.AF_UNSPEC, socket.SOCK_STREAM
        )
        resolved_gcs_host = addrinfo[0][4][0]
    except socket.gaierror:
        raise click.ClickException(f"Could not resolve hostname: {gcs_host}")

    my_ips = []
    for iface, addrs in psutil.net_if_addrs().items():
        for addr in addrs:
            # Look for AF_INET (IPv4) or AF_INET6 (IPv6)
            if addr.family in [
                socket.AddressFamily.AF_INET,
                socket.AddressFamily.AF_INET6,
            ]:
                my_ips.append(addr.address)

    if min_nodes > 1:
        # Ban localhost ips if we are not running on a single node
        # to avoid starting N head nodes
        my_ips = [ip for ip in my_ips if ip != "127.0.0.1" and ip != "::1"]

    is_head = resolved_gcs_host in my_ips

    result = None
    # 2. Start Ray and run commands.
    try:
        if is_head:
            # On the head node, start Ray, run the command, then stop Ray.
            click.echo("On head node. Starting Ray cluster head...")

            # Build the ray start command with all parameters
            ray_start_cmd = [
                "ray",
                "start",
                "--head",
                f"--node-ip-address={resolved_gcs_host}",
                f"--port={gcs_port}",
                *ray_start_args,
            ]

            # Start Ray head. This runs in the background and hides output.
            subprocess.run(ray_start_cmd, check=True, capture_output=True)
            click.echo("Head node started.")
            click.echo("=======================")
            if min_nodes > 1 and not check_cluster_ready(min_nodes):
                raise click.ClickException(
                    "Timed out waiting for other nodes to start."
                )

            click.echo(
                f"Running command on head node: {entrypoint_on_head}",
            )
            click.echo("=======================")
            result = subprocess.run(entrypoint_on_head)
            click.echo("=======================")
        else:
            # On a worker node, start Ray and connect to the head.
            click.echo(f"On worker node. Connecting to Ray cluster at {address}...")

            if not check_head_node_ready(address):
                raise click.ClickException("Timed out waiting for head node to start.")

            # Build the ray start command for worker nodes with all parameters
            ray_start_cmd = [
                "ray",
                "start",
                "--address",
                address,
                "--block",
                *ray_start_args,
            ]

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
