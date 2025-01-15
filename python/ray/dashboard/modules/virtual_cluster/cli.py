import json
import logging
import sys
from typing import Optional

import click

import ray._private.services as services
from ray._private.gcs_utils import GcsChannel
from ray.autoscaler._private.cli_logger import add_click_logging_options, cli_logger
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated.gcs_service_pb2 import (
    CreateOrUpdateVirtualClusterRequest,
    GetVirtualClustersRequest,
    RemoveVirtualClusterRequest,
)

logger = logging.getLogger(__name__)


def _get_virtual_cluster_stub(address: Optional[str] = None):
    address = services.canonicalize_bootstrap_address_or_die(address)
    channel = GcsChannel(address, aio=False)
    channel.connect()
    return gcs_service_pb2_grpc.VirtualClusterInfoGcsServiceStub(channel.channel())


@click.group("vcluster")
def vclusters_cli_group():
    """Create ,update or remove virtual clusters."""
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
