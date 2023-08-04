import grpc

# Users need to define their custom message and response protobuf
from user_defined_protos_pb2 import FruitAmounts, FruitCosts

# Serve will provide stub for users to use
from ray.serve._private.grpc_util import RayServeServiceStub

# Port default to 9001. Port will be configurable in the future
channel = grpc.insecure_channel("localhost:9001")

# UserDefinedResponse is used to deserialize the response from the server
stub = RayServeServiceStub(channel, FruitCosts)

test_in = FruitAmounts(
    orange=4,
    apple=8,
)
metadata = (
    ("route_path", "/"),
    # ("method_name", "method1"),
    # ("application", "default_grpc-deployment"),
    # ("request_id", "123"),  # Optional, feature parity w/ http proxy
    # ("multiplexed_model_id", "456"),  # Optional, feature parity w/ http proxy
)

# Predict method is defined by Serve's gRPC service use to return unary response
response, call = stub.Predict.with_call(request=test_in, metadata=metadata)

print(call.trailing_metadata())  # Request id is returned in the trailing metadata
print("Output type:", type(response))  # Response is a type of FruitCosts
print("Full output:", response)
print("Output costs field:", response.costs)
