import grpc
from google.protobuf.any_pb2 import Any

from ray.serve.generated import serve_pb2, serve_pb2_grpc

channel = grpc.insecure_channel("localhost:8001")
stub = serve_pb2_grpc.RayServeServiceStub(channel)

test_in = Any()
test_in.Pack(serve_pb2.TestIn(name="genesu"))
response = stub.Predict(
    serve_pb2.RayServeRequest(
        application="default_test-name",
        user_request=test_in,
        request_id="123",
        # multiplexed_model_id="123",
    )
)
print("Output type:", type(response.user_response))
print("Full output:", response.user_response)
print("request_id:", response.request_id)

test_out = serve_pb2.TestOut()
response.user_response.Unpack(test_out)
print("Output greeting field:", test_out.greeting)
