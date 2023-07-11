import grpc
from google.protobuf.any_pb2 import Any

from ray.serve.generated import serve_pb2, serve_pb2_grpc

channel = grpc.insecure_channel("localhost:8001")
stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)

test_in = Any()
test_in.Pack(serve_pb2.TestIn(name="eoakes"))
response = stub.Predict(
    serve_pb2.PredictRequest(target="default_test-name", input=test_in)
)
print("Output type:", type(response.output))
print("Full output:", response.output)

test_out = serve_pb2.TestOut()
response.output.Unpack(test_out)
print("Output greeting field:", test_out.greeting)


test_in = Any()
test_in.Pack(serve_pb2.TestIn(name="genesu"))
response = stub.Predict(
    serve_pb2.PredictRequest(target="default_test-name", input=test_in)
)
print("Output type:", type(response.output))
print("Full output:", response.output)

test_out = serve_pb2.TestOut()
response.output.Unpack(test_out)
print("Output greeting field:", test_out.greeting)
