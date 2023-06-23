from ray.core.generated.experimental import autoscaler_pb2


def get_cluster_resource_state(stub) -> autoscaler_pb2.ClusterResourceState:
    request = autoscaler_pb2.GetClusterResourceStateRequest(
        last_seen_cluster_resource_state_version=0
    )
    return stub.GetClusterResourceState(request).cluster_resource_state


def report_autoscaling_state(stub, pending_requests: dict) -> autoscaler_pb2.ReportAutoscalingStateReply:
    autoscaling_state = autoscaler_pb2.AutoscalingState()
    for pending_request in pending_requests:
        autoscaling_state.pending_instance_requests.append(autoscaler_pb2.PendingInstanceRequest(
            instance_type_name=pending_request["instance_type_name"],
            count=pending_request["count"],
            ray_node_type_name=pending_request["ray_node_type_name"],
        ))

    request = autoscaler_pb2.ReportAutoscalingStateRequest(autoscaling_state=autoscaling_state)

    return stub.ReportAutoscalingState(request)