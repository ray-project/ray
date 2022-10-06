# flake8: noqa

# __begin_server__
from ray import serve
from ray.serve.drivers import DefaultgRPCDriver


@serve.deployment
class D1:
    def __call__(self, input):
        # Do something with input
        return input["a"]


my_deployment = DefaultgRPCDriver.bind(D1.bind())

serve.run(my_deployment)
# __end_server__

from ray.serve.generated import serve_pb2, serve_pb2_grpc

# __begin_client__
import grpc

channel = grpc.aio.insecure_channel("localhost:9000")
stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
response = stub.Predict(serve_pb2.PredictRequest(input={"a": bytes("123", "utf-8")}))
# __end_client__
