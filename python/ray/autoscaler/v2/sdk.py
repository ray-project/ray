from typing import List

import ray

DEFAULT_RPC_TIMEOUT_S = 10


def get_gcs_client():
    """Get GCS client from the worker"""
    return ray.worker.global_worker.node.get_gcs_client()


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
    get_gcs_client().request_cluster_resource_constraint(to_request, timeout=timeout)
