import time
from collections import defaultdict
from typing import List

from ray._raylet import GcsClient
from ray.autoscaler.v2.schema import ClusterStatus, Stats
from ray.autoscaler.v2.utils import ClusterStatusParser
from ray.core.generated.autoscaler_pb2 import GetClusterStatusReply

DEFAULT_RPC_TIMEOUT_S = 10


def request_cluster_resources(
    gcs_address: str, to_request: List[dict], timeout: int = DEFAULT_RPC_TIMEOUT_S
):
    """Request resources from the autoscaler.

    This will add a cluster resource constraint to GCS. GCS will asynchronously
    pass the constraint to the autoscaler, and the autoscaler will try to provision the
    requested minimal bundles in `to_request`.

    If the cluster already has `to_request` resources, this will be an no-op.
    Future requests submitted through this API will overwrite the previous requests.

    Args:
        gcs_address: The GCS address to query.
        to_request: A list of resource bundles to request the cluster to have.
            Each bundle is a dict of resource name to resource quantity, e.g:
            [{"CPU": 1}, {"GPU": 1}].
        timeout: Timeout in seconds for the request to be timeout

    """
    assert len(gcs_address) > 0, "GCS address is not specified."

    # Aggregate bundle by shape.
    resource_requests_by_count = defaultdict(int)
    for request in to_request:
        bundle = frozenset(request.items())
        resource_requests_by_count[bundle] += 1

    bundles = []
    counts = []
    for bundle, count in resource_requests_by_count.items():
        bundles.append(dict(bundle))
        counts.append(count)

    GcsClient(gcs_address).request_cluster_resource_constraint(
        bundles, counts, timeout_s=timeout
    )


def get_cluster_status(
    gcs_address: str, timeout: int = DEFAULT_RPC_TIMEOUT_S
) -> ClusterStatus:
    """
    Get the cluster status from the autoscaler.

    Args:
        gcs_address: The GCS address to query.
        timeout: Timeout in seconds for the request to be timeout

    Returns:
        A ClusterStatus object.
    """
    assert len(gcs_address) > 0, "GCS address is not specified."
    req_time = time.time()
    str_reply = GcsClient(gcs_address).get_cluster_status(timeout_s=timeout)
    reply_time = time.time()
    reply = GetClusterStatusReply()
    reply.ParseFromString(str_reply)

    # TODO(rickyx): To be more accurate, we could add a timestamp field from the reply.
    return ClusterStatusParser.from_get_cluster_status_reply(
        reply,
        stats=Stats(gcs_request_time_s=reply_time - req_time, request_ts_s=req_time),
    )
