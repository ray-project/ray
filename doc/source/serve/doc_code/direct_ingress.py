# flake8: noqa

# __begin_server__
import ray
from ray import serve
from ray.serve.drivers import DefaultgRPCDriver
from ray.serve.handle import RayServeDeploymentHandle
from ray.serve.deployment_graph import InputNode
from typing import Dict
import struct


@serve.deployment
class FruitMarket:
    def __init__(
        self,
        orange_stand: RayServeDeploymentHandle,
        apple_stand: RayServeDeploymentHandle,
    ):
        self.directory = {
            "ORANGE": orange_stand,
            "APPLE": apple_stand,
        }

    async def check_price(self, inputs: Dict[str, bytes]) -> float:
        costs = 0
        for fruit, amount in inputs.items():
            if fruit not in self.directory:
                return
            fruit_stand = self.directory[fruit]
            ref: ray.ObjectRef = await fruit_stand.remote(int(amount))
            result = await ref
            costs += result
        return bytearray(struct.pack("f", costs))


@serve.deployment
class OrangeStand:
    def __init__(self):
        self.price = 2.0

    def __call__(self, num_oranges: int):
        return num_oranges * self.price


@serve.deployment
class AppleStand:
    def __init__(self):
        self.price = 3.0

    def __call__(self, num_oranges: int):
        return num_oranges * self.price


with InputNode() as input:
    orange_stand = OrangeStand.bind()
    apple_stand = AppleStand.bind()
    fruit_market = FruitMarket.bind(orange_stand, apple_stand)
    my_deployment = DefaultgRPCDriver.bind(fruit_market.check_price.bind(input))

serve.run(my_deployment)
# __end_server__

# __begin_client__
import grpc
from ray.serve.generated import serve_pb2, serve_pb2_grpc
import asyncio
import struct


async def send_request():
    async with grpc.aio.insecure_channel("localhost:9000") as channel:
        stub = serve_pb2_grpc.PredictAPIsServiceStub(channel)
        response = await stub.Predict(
            serve_pb2.PredictRequest(
                input={"ORANGE": bytes("10", "utf-8"), "APPLE": bytes("3", "utf-8")}
            )
        )
    return response


async def main():
    resp = await send_request()
    print(struct.unpack("f", resp.prediction))


# for python>=3.7, please use asyncio.run(main())
asyncio.get_event_loop().run_until_complete(main())

# __end_client__
