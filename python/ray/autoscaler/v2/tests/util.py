from ray.core.generated.experimental import autoscaler_pb2, autoscaler_pb2_grpc

def get_cluster_resource_state(stub) -> autoscaler_pb2.GetClusterResourceStateReply:
    request = autoscaler_pb2.GetClusterResourceStateRequest(last_seen_cluster_resource_state_version=0)
    return stub.GetClusterResourceState(request)