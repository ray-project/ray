import grpc

# Users need to define their custom message and response protobuf
from user_defined_protos_pb2 import UserDefinedMessage, UserDefinedResponse

# Serve will provide stub for users to use
from ray.serve._private.grpc_util import RayServeServiceStub

# Port default to 9001. Port will be configurable in the future
channel = grpc.insecure_channel("localhost:9001")

# UserDefinedResponse is used to deserialize the response from the server
stub = RayServeServiceStub(channel, UserDefinedResponse)

test_in = UserDefinedMessage(
    name="genesu",
    num=88,
    foo="bar",
)
metadata = (
    ("route_path", "/"),
    ("method_name", "streaming"),
    # ("application", "default_grpc-deployment"),
    # ("request_id", "123"),  # Optional, feature parity w/ http proxy
    # ("multiplexed_model_id", "456"),  # Optional, feature parity w/ http proxy
)

# PredictStreaming method is defined by Serve's gRPC service use to return
# streaming response
responses = stub.PredictStreaming(test_in, metadata=metadata)

for response in responses:
    print("Output type:", type(response))  # Response is a type of UserDefinedResponse
    print("Full output:", response)
    print("Output greeting field:", response.greeting)
    print("Output num_x2 field:", response.num_x2)
print(responses.trailing_metadata())  # Request id is returned in the trailing metadata
