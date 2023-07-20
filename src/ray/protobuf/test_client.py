import grpc

from ray.serve.generated import serve_pb2, serve_pb2_grpc

channel = grpc.insecure_channel("localhost:9000")
stub = serve_pb2_grpc.RayServeServiceStub(channel)

test_in = serve_pb2.TestIn(
    name="genesu",
    num=88,
    foo="bar",
)
# NOTE: user_request must be serialized to bytes
user_request = test_in.SerializeToString()
response = stub.Predict(
    serve_pb2.RayServeRequest(
        application="default_test-name",
        user_request=user_request,
        request_id="123",
        # multiplexed_model_id="123",
    )
)
print("Output type:", type(response.user_response))
print("Full output:", response.user_response)
print("request_id:", response.request_id)

test_out = serve_pb2.TestOut()
test_out.ParseFromString(response.user_response)
print("Output greeting field:", test_out.greeting)
print("Output num_x2 field:", test_out.num_x2)
