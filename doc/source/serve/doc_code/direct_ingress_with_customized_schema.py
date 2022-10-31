# flake8: noqa

# __begin_server__
from ray import serve
from ray.serve.drivers import gRPCIngress
import test_service_pb2_grpc, test_service_pb2


@serve.deployment(is_driver_deployment=True)
class MyDriver(test_service_pb2_grpc.TestServiceServicer, gRPCIngress):
    def __init__(self):
        super().__init__()

    async def Ping(self, request, context):
        # play with your dag and then reply
        return test_service_pb2.PingReply()


my_deployment = MyDriver.bind()

serve.run(my_deployment)
# __end_server__


# __begin_client__
import grpc
import test_service_pb2_grpc, test_service_pb2

channel = grpc.aio.insecure_channel("localhost:9000")
stub = test_service_pb2_grpc.TestServiceStub(channel)
response = stub.Ping(test_service_pb2.PingRequest())
# __end_client__
