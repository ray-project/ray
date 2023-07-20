from ray.core.generated import autoscaler_pb2


def get_cluster_resource_state(stub) -> autoscaler_pb2.ClusterResourceState:
    request = autoscaler_pb2.GetClusterResourceStateRequest(
        last_seen_cluster_resource_state_version=0
    )
    return stub.GetClusterResourceState(request).cluster_resource_state
