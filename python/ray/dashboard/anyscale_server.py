from concurrent import futures
import logging

import grpc
import ray
from ray.core.generated import dashboard_pb2
from ray.core.generated import dashboard_pb2_grpc
import redis

# TODO: Start a redis server here for the dashboard to connect to.
# For now, just assume on is running on port 6379.

# Need to start redis with "redis-server" and the dashboard with
# "python dashboard.py --host 127.0.0.1 --port 3000 --redis-address 127.0.0.1:6379"

# redis_client = redis.StrictRedis(host="127.0.0.1", port=6379)

class HostedDashboardServer(dashboard_pb2_grpc.DashboardServiceServicer):
    """Service that a Ray cluster can connect to in order to push all its metrics."""

    def __init__(self):
        pass

    def NodeInfoEvent(self, request, context):
        # redis_client.publish(ray.gcs_utils.REPORTER_CHANNEL + ".", request.json_data)
        print('node info event: {}'.format(request.json_data))
        return dashboard_pb2.NodeInfoEventReply()

    def RayletInfoEvent(self, request, context):
        # redis_client.publish(ray.gcs_utils.REPORTER_CHANNEL + ".", request.json_data)
        print('raylet info event: {}'.format(request.json_data))
        return dashboard_pb2.RayletInfoEventReply()

    def LogFileEvent(self, request, context):
        # redis_client.publish(ray.gcs_utils.REPORTER_CHANNEL + ".", request.json_data)
        print('log file event: {}'.format(request.json_data))
        return dashboard_pb2.LogFileEventReply()

    def ErrorInfoEvent(self, request, context):
        # redis_client.publish(ray.gcs_utils.LOG_FILE_CHANNEL, request.json_data)
        print('error info event: {}'.format(request.json_data))
        return dashboard_pb2.ErrorInfoEventReply()

    def ProfilingStatusEvent(self, request, context):
        # error_channel = ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")
        # redis_client.publish(error_channel, request.gcs_data)
        print('profiling status event: {}'.format(request.json_data))
        return dashboard_pb2.ProfilingStatusEventReply()

    def ProfilingInfoEvent(self, request, context):
        # actor_channel = ray.gcs_utils.TablePubsub.Value("ACTOR_PUBSUB")
        # redis_client.publish(error_channel, request.gcs_data)
        print('profiling info event: {}'.format(request.json_data))
        return dashboard_pb2.ProfilingInfoReply()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dashboard_pb2_grpc.add_DashboardServiceServicer_to_server(
        HostedDashboardServer(), server)
    port = 50051
    server.add_insecure_port("[::]:{}".format(port))
    print("Server listening on port {}".format(port))
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve() 