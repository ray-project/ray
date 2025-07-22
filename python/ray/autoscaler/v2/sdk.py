import time
from collections import Counter
from typing import List, NamedTuple

from ray._raylet import GcsClient
from ray.autoscaler.v2.schema import ClusterStatus, Stats
from ray.autoscaler.v2.utils import ClusterStatusParser
from ray.core.generated.autoscaler_pb2 import (
    ClusterResourceState,
    GetClusterResourceStateReply,
    GetClusterStatusReply,
)

DEFAULT_RPC_TIMEOUT_S = 10


class ResourceRequest(NamedTuple):
    resources: dict
    label_selectors: List[dict]


def request_cluster_resources(
    gcs_address: str,
    to_request: List[ResourceRequest],
    timeout: int = DEFAULT_RPC_TIMEOUT_S,
):
    """Request resources from the autoscaler.

    This will add a cluster resource constraint to GCS. GCS will asynchronously
    pass the constraint to the autoscaler, and the autoscaler will try to provision the
    requested minimal bundles in `to_request`.

    If the cluster already has `to_request` resources, this will be an no-op.
    Future requests submitted through this API will overwrite the previous requests.

    Args:
        gcs_address: The GCS address to query.
        to_request: A list of resource requests to request the cluster to have.
            Each resource request is a tuple of resource bundles and label_selectors
            to apply per-bundle. Each bundle is a dict of resource name to resource
            quantity, e.g: [{"CPU": 1}, {"GPU": 1}].
        timeout: Timeout in seconds for the request to be timeout

    """
    assert len(gcs_address) > 0, "GCS address is not specified."

    # Convert resource bundle dicts to ResourceRequest tuples if necessary
    normalized: List[ResourceRequest] = []
    for r in to_request:
        if isinstance(r, ResourceRequest):
            normalized.append(r)
        elif isinstance(r, dict):
            if "resources" in r:
                resources = r["resources"]
                selectors = r.get("label_selectors", [])
            else:
                resources = r
                selectors = []
            if isinstance(selectors, dict):
                selectors = [selectors]
            normalized.append(ResourceRequest(resources, selectors))
        else:
            raise TypeError("Each element must be ResourceRequest or dict")
    to_request = normalized

    # Aggregate bundle by shape.
    def keyfunc(r):
        return (
            frozenset(r.resources.items()),
            tuple(frozenset(m.items()) for m in r.label_selectors),
        )

    grouped_requests = Counter(keyfunc(r) for r in to_request)
    bundles: List[dict] = []
    label_selectors: List[List[dict]] = []
    counts: List[int] = []

    for (bundle, selectors), count in grouped_requests.items():
        bundles.append(dict(bundle))
        label_selectors.append([dict(sel) for sel in selectors])
        counts.append(count)

    GcsClient(gcs_address).request_cluster_resource_constraint(
        bundles, counts, label_selectors, timeout_s=timeout
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


def get_cluster_resource_state(gcs_client: GcsClient) -> ClusterResourceState:
    """
    Get the cluster resource state from GCS.
    Args:
        gcs_client: The GCS client to query.
    Returns:
        A ClusterResourceState object
    Raises:
        Exception: If the request times out or failed.
    """
    str_reply = gcs_client.get_cluster_resource_state()
    reply = GetClusterResourceStateReply()
    reply.ParseFromString(str_reply)
    return reply.cluster_resource_state
