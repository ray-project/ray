import time

import grpc
from google.protobuf.any_pb2 import Any as AnyProto

from ray.serve.generated.serve_pb2 import TestIn, TestOut
from ray.serve.generated.serve_pb2_grpc import RayServeServiceStub

start_time = time.time()
channel = grpc.insecure_channel("localhost:9000")
stub = RayServeServiceStub(channel)

test_in = TestIn(
    name="genesu",
    num=88,
    foo="bar",
)
test_in_any = AnyProto()
test_in_any.Pack(test_in)
metadata = [
    ("route_path", "/"),
    ("method_name", "method1"),
    # ("application", "default_grpc-deployment"),
    # ("request_id", "123"),
    # ("multiplexed_model_id", "456"),
]
response, call = stub.Predict.with_call(request=test_in_any, metadata=metadata)
print(call.trailing_metadata())
print("Time taken:", time.time() - start_time)
print("Output type:", type(response))
print("Full output:", response)

test_out = TestOut()
response.Unpack(test_out)
print("Output greeting field:", test_out.greeting)
print("Output num_x2 field:", test_out.num_x2)
