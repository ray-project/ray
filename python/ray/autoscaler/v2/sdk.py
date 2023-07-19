from typing import List, Optional

import ray
from ray._raylet import GcsClient
from ray.core.generated.experimental.autoscaler_pb2 import GetClusterStatusReply

DEFAULT_RPC_TIMEOUT_S = 10


def get_gcs_client(gcs_address: Optional[str] = None):
    """Get the GCS client."""
    if gcs_address is None:
        gcs_address = ray.get_runtime_context().gcs_address
    assert gcs_address is not None, (
        "GCS address should have been detected if getting from runtime context"
        "or passed in explicitly."
    )
    return GcsClient(address=gcs_address)


def request_cluster_resources(
    to_request: List[dict], timeout: int = DEFAULT_RPC_TIMEOUT_S
):
    """Request resources from the autoscaler.

    This will add a cluster resource constraint to GCS. GCS will asynchronously
    pass the constraint to the autoscaler, and the autoscaler will try to provision the
    requested minimal bundles in `to_request`.

    If the cluster already has `to_request` resources, this will be an no-op.
    Future requests submitted through this API will overwrite the previous requests.

    NOTE:
        This function has to be invoked in a ray worker/driver, i.e., after `ray.init()`

    Args:
        to_request: A list of resource bundles to request the cluster to have.
            Each bundle is a dict of resource name to resource quantity, e.g:
            [{"CPU": 1}, {"GPU": 1}].
        timeout: Timeout in seconds for the request to be timeout

    """
    get_gcs_client().request_cluster_resource_constraint(to_request, timeout_s=timeout)


def _get_cluster_status(
    gcs_address: Optional[str] = None, timeout: int = DEFAULT_RPC_TIMEOUT_S
) -> GetClusterStatusReply:
    """
    Get the cluster status from the autoscaler.

    TODO(rickyx): Parse it into a ClusterStatus object.
    """

    str_reply = get_gcs_client(gcs_address).get_cluster_status(timeout_s=timeout)
    reply = GetClusterStatusReply()
    reply.ParseFromString(str_reply)

    return reply
