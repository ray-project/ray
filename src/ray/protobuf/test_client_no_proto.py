import pickle
import time

import grpc

from ray.serve.generated import serve_pb2, serve_pb2_grpc

start_time = time.time()
channel = grpc.insecure_channel("localhost:9000")
stub = serve_pb2_grpc.RayServeServiceStub(channel)

test_in = {
    "name": "genesu",
    "num": 88,
    "foo": "bar",
}
response = stub.Predict(
    serve_pb2.RayServeRequest(
        application="default_grpc-deployment-no-proto",
        user_request=pickle.dumps(test_in),
        request_id="123",
        # multiplexed_model_id="123",
    )
)
print("Time taken:", time.time() - start_time)
print("Output type:", type(response.user_response))
print("Full output:", response.user_response)
print("request_id:", response.request_id)

test_out = pickle.loads(response.user_response)
print("Output greeting field:", test_out["greeting"])
print("Output num_x2 field:", test_out["num_x2"])
