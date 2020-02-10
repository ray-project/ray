import logging

from concurrent import futures

import grpc
import ray
import redis

from ray.core.generated import dashboard_pb2
from ray.core.generated import dashboard_pb2_grpc

# TODO(sang): Move it to a different file.
NODE_INFO_CHANNEL = "NODE_INFO_CHANNEL"
RAY_INFO_CHANNEL = "RAY_INFO_CHANNEL"


class HostedDashboardServer(dashboard_pb2_grpc.DashboardServiceServicer):
    """Service that a Ray cluster can connect to in order to push all its metrics."""

    def __init__(self):
        self.redis_client = redis.StrictRedis(host="127.0.0.1", port=6379)

    def NodeInfoEvent(self, request, context):
        result = self.redis_client.publish(NODE_INFO_CHANNEL, request.json_data)
        print("nodeinfo subscriber: {}".format(result))
        return dashboard_pb2.NodeInfoEventReply()

    def RayletInfoEvent(self, request, context):
        result = self.redis_client.publish(RAY_INFO_CHANNEL, request.json_data)
        print("raylet subscriber: {}".format(result))
        return dashboard_pb2.RayletInfoEventReply()

    def LogFileEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        # redis_client.publish(ray.gcs_utils.REPORTER_CHANNEL + ".", request.json_data)
        # print('log file event: {}'.format(request.json_data))
        return dashboard_pb2.LogFileEventReply()

    def ErrorInfoEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        # redis_client.publish(ray.gcs_utils.LOG_FILE_CHANNEL, request.json_data)
        # print('error info event: {}'.format(request.json_data))
        return dashboard_pb2.ErrorInfoEventReply()

    def ProfilingStatusEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        # error_channel = ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")
        # redis_client.publish(error_channel, request.gcs_data)
        # print('profiling status event: {}'.format(request.json_data))
        return dashboard_pb2.ProfilingStatusEventReply()

    def ProfilingInfoEvent(self, request, context):
        # TODO(sang): Implement this and test it.
        # actor_channel = ray.gcs_utils.TablePubsub.Value("ACTOR_PUBSUB")
        # redis_client.publish(error_channel, request.gcs_data)
        # print('profiling info event: {}'.format(request.json_data))
        return dashboard_pb2.ProfilingInfoReply()


if __name__ == '__main__':
    logging.basicConfig()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dashboard_pb2_grpc.add_DashboardServiceServicer_to_server(
        HostedDashboardServer(), server)
    port = 50051
    server.add_insecure_port("[::]:{}".format(port))
    print("Server listening on port {}".format(port))
    server.start()
    server.wait_for_termination()