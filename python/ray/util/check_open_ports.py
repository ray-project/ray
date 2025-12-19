"""A CLI utility for check open ports in the Ray cluster.

See https://www.anyscale.com/blog/update-on-ray-cve-2023-48022-new-verification-tooling-available # noqa: E501
for more details.
"""
import json
import subprocess
import urllib
from typing import List, Tuple

import click

import ray
from ray.autoscaler._private.cli_logger import add_click_logging_options, cli_logger
from ray.autoscaler._private.constants import RAY_PROCESSES
from ray.util.annotations import PublicAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

import psutil


def _get_ray_ports() -> List[int]:
    unique_ports = set()

    process_infos = []
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            process_infos.append((proc, proc.name(), proc.cmdline()))
        except psutil.Error:
            pass

    for keyword, filter_by_cmd in RAY_PROCESSES:
        for candidate in process_infos:
            proc, proc_cmd, proc_args = candidate
            corpus = proc_cmd if filter_by_cmd else subprocess.list2cmdline(proc_args)
            if keyword in corpus:
                try:
                    for connection in proc.connections():
                        if connection.status == psutil.CONN_LISTEN:
                            unique_ports.add(connection.laddr.port)
                except psutil.AccessDenied:
                    cli_logger.info(
                        "Access denied to process connections for process,"
                        " worker process probably restarted",
                        proc,
                    )

    return sorted(unique_ports)


def _check_for_open_ports_from_internet(
    service_url: str, ports: List[int]
) -> Tuple[List[int], List[int]]:
    request = urllib.request.Request(
        method="POST",
        url=service_url,
        headers={
            "Content-Type": "application/json",
            "X-Ray-Open-Port-Check": "1",
        },
        data=json.dumps({"ports": ports}).encode("utf-8"),
    )

    response = urllib.request.urlopen(request)
    if response.status != 200:
        raise RuntimeError(
            f"Failed to check with Ray Open Port Service: {response.status}"
        )
    response_body = json.load(response)

    publicly_open_ports = response_body.get("open_ports", [])
    checked_ports = response_body.get("checked_ports", [])

    return publicly_open_ports, checked_ports


def _check_if_exposed_to_internet(
    service_url: str,
) -> Tuple[List[int], List[int]]:
    return _check_for_open_ports_from_internet(service_url, _get_ray_ports())


def _check_ray_cluster(
    service_url: str,
) -> List[Tuple[str, Tuple[List[int], List[int]]]]:
    ray.init(ignore_reinit_error=True)

    @ray.remote(num_cpus=0)
    def check(node_id, service_url):
        return node_id, _check_if_exposed_to_internet(service_url)

    ray_node_ids = [node["NodeID"] for node in ray.nodes() if node["Alive"]]
    cli_logger.info(
        f"Cluster has {len(ray_node_ids)} node(s)."
        " Scheduling tasks on each to check for exposed ports",
    )

    per_node_tasks = {
        node_id: (
            check.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node_id, soft=False
                )
            ).remote(node_id, service_url)
        )
        for node_id in ray_node_ids
    }

    results = []
    for node_id, per_node_task in per_node_tasks.items():
        try:
            results.append(ray.get(per_node_task))
        except Exception as e:
            cli_logger.info(f"Failed to check on node {node_id}: {e}")

    return results


@click.command()
@click.option(
    "--yes", "-y", is_flag=True, default=False, help="Don't ask for confirmation."
)
@click.option(
    "--service-url",
    required=False,
    type=str,
    default="https://ray-open-port-checker.uc.r.appspot.com/open-port-check",
    help="The url of service that checks whether submitted ports are open.",
)
@add_click_logging_options
@PublicAPI
def check_open_ports(yes, service_url):
    """Check open ports in the local Ray cluster."""
    if not cli_logger.confirm(
        yes=yes,
        msg=(
            "Do you want to check the local Ray cluster"
            " for any nodes with ports accessible to the internet?"
        ),
        _default=True,
    ):
        cli_logger.info("Exiting without checking as instructed")
        return

    cluster_open_ports = _check_ray_cluster(service_url)

    public_nodes = []
    for node_id, (open_ports, checked_ports) in cluster_open_ports:
        if open_ports:
            cli_logger.info(
                f"[🛑] open ports detected open_ports={open_ports!r} node={node_id!r}"
            )
            public_nodes.append((node_id, open_ports, checked_ports))
        else:
            cli_logger.info(
                f"[🟢] No open ports detected "
                f"checked_ports={checked_ports!r} node={node_id!r}"
            )

    cli_logger.info("Check complete, results:")

    if public_nodes:
        cli_logger.info(
            """
[🛑] An server on the internet was able to open a connection to one of this Ray
cluster's public IP on one of Ray's internal ports. If this is not a false
positive, this is an extremely unsafe configuration for Ray to be running in.
Ray is not meant to be exposed to untrusted clients and will allow them to run
arbitrary code on your machine.

You should take immediate action to validate this result and if confirmed shut
down your Ray cluster immediately and take appropriate action to remediate its
exposure. Anything either running on this Ray cluster or that this cluster has
had access to could be at risk.

For guidance on how to operate Ray safely, please review [Ray's security
documentation](https://docs.ray.io/en/latest/ray-security/index.html).
""".strip()
        )
    else:
        cli_logger.info("[🟢] No open ports detected from any Ray nodes")
