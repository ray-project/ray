import pickle
import struct
import time

import grpc

from ray.serve.generated import serve_pb2, serve_pb2_grpc

start_time = time.time()
channel = grpc.insecure_channel("localhost:9000")
stub = serve_pb2_grpc.RayServeServiceStub(channel)

input = {
    "ORANGE": 10,
    "APPLE": 3,
}
response = stub.Predict(
    serve_pb2.RayServeRequest(
        application="default_grpc-deployment-multi-app",
        user_request=pickle.dumps(input),
        request_id="123",
        # multiplexed_model_id="123",
    )
)
print("Time taken:", time.time() - start_time)
print("Output type:", type(response.user_response))
print("Full output:", response.user_response)
print("request_id:", response.request_id)

response = struct.unpack("f", response.user_response)
print("Output:", response)
