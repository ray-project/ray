import asyncio

import grpc

import test_service_pb2
import test_service_pb2_grpc

from ray.serve.generated import serve_pb2, serve_pb2_grpc


async def run() -> None:

    # internal schema
    async with grpc.aio.insecure_channel("localhost:50001") as channel:
        stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
        response = await stub.Predict(
            serve_pb2.PredictRequest(input={"a": bytes("123", "utf-8")})
        )
    print(response.prediction)

    """
    # customer schema
    async with grpc.aio.insecure_channel("localhost:50001") as channel:
        stub = test_service_pb2_grpc.TestServiceStub(channel)
        response = await stub.Ping(test_service_pb2.PingRequest())
    print(response)
    """


if __name__ == "__main__":
    asyncio.run(run())
