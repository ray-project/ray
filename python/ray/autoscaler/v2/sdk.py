from typing import List

import ray
import ray._private.ray_constants as ray_constants
from ray.core.generated.experimental import autoscaler_pb2, autoscaler_pb2_grpc


def _autoscaler_state_service_stub():
    """Get the grpc stub for the autoscaler state service"""
    gcs_address = ray.get_runtime_context().gcs_address
    gcs_channel = ray._private.utils.init_grpc_channel(
        gcs_address, ray_constants.GLOBAL_GRPC_OPTIONS
    )
    return autoscaler_pb2_grpc.AutoscalerStateServiceStub(gcs_channel)


def request_cluster_resources(to_request: List[dict], timeout: int = 10):
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

    # NOTE: We could also use a GCS python client. However, current GCS rpc client
    # expects GcsStatus as part of the reply, which is a protocol internal to Ray.
    # So we use the rpc stub directly to avoid that dependency.
    stub = _autoscaler_state_service_stub()
    min_bundles = [
        autoscaler_pb2.ResourceRequest(resources_bundle=bundle) for bundle in to_request
    ]
    request = autoscaler_pb2.RequestClusterResourceConstraintRequest(
        cluster_resource_constraint=autoscaler_pb2.ClusterResourceConstraint(
            min_bundles=min_bundles
        )
    )

    stub.RequestClusterResourceConstraint(request, timeout=timeout)
