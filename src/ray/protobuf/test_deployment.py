import pickle
import struct
import time
from typing import Dict, Generator

from starlette.requests import Request

import ray
from ray import serve
from ray.serve.generated import serve_pb2
from ray.serve.handle import RayServeDeploymentHandle


@serve.deployment
class GrpcDeployment:
    def __call__(self, my_input: bytes) -> bytes:
        request = serve_pb2.TestIn()
        request.ParseFromString(my_input)
        greeting = f"Hello {request.name} from {request.foo}"
        num_x2 = request.num * 2
        output = serve_pb2.TestOut(
            greeting=greeting,
            num_x2=num_x2,
        )
        return output.SerializeToString()


g = GrpcDeployment.options(name="grpc-deployment").bind()


@serve.deployment
class GrpcDeploymentNoProto:
    def __call__(self, my_input: bytes) -> bytes:
        request = pickle.loads(my_input)
        greeting = f"Hello {request['name']} from {request['foo']}"
        num_x2 = request["num"] * 2
        output = {
            "greeting": greeting,
            "num_x2": num_x2,
        }
        return pickle.dumps(output)


g2 = GrpcDeploymentNoProto.options(name="grpc-deployment-no-proto").bind()


@serve.deployment
class GrpcDeploymentStreamingResponse:
    def __call__(self, my_input: bytes) -> Generator[bytes, None, None]:
        print("my_input", my_input)
        request = serve_pb2.TestIn()
        request.ParseFromString(my_input)
        for i in range(10):
            greeting = f"{i}: Hello {request.name} from {request.foo}"
            num_x2 = request.num * 2 + i
            output = serve_pb2.TestOut(
                greeting=greeting,
                num_x2=num_x2,
            )
            yield output.SerializeToString()

            time.sleep(0.1)


g3 = GrpcDeploymentStreamingResponse.options(
    name="grpc-deployment-streaming-response"
).bind()


@serve.deployment
class FruitMarket:
    def __init__(
        self,
        _orange_stand: RayServeDeploymentHandle,
        _apple_stand: RayServeDeploymentHandle,
    ):
        self.directory = {
            "ORANGE": _orange_stand,
            "APPLE": _apple_stand,
        }

    async def __call__(self, inputs: bytes) -> bytes:
        fruit_amounts = pickle.loads(inputs)
        costs = await self.check_price(fruit_amounts)
        return struct.pack("f", costs)

    async def check_price(self, inputs: Dict[str, int]) -> float:
        costs = 0
        for fruit, amount in inputs.items():
            if fruit not in self.directory:
                return
            fruit_stand = self.directory[fruit]
            ref: ray.ObjectRef = await fruit_stand.remote(int(amount))
            result = await ref
            costs += result
        return costs


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


orange_stand = OrangeStand.bind()
apple_stand = AppleStand.bind()
g4 = FruitMarket.options(name="grpc-deployment-multi-app").bind(
    orange_stand, apple_stand
)


@serve.deployment
class GrpcDeploymentMultiplexing:
    @serve.multiplexed(max_num_models_per_replica=3)
    async def get_model(self, model_id: str) -> str:
        return f"loading model: {model_id}"

    async def __call__(self, request: bytes) -> bytes:
        model_id = serve.get_multiplexed_model_id()
        model = await self.get_model(model_id)
        return model.encode("utf-8")


g5 = GrpcDeploymentMultiplexing.options(name="grpc-deployment-multiplexing").bind()


@serve.deployment
class HttpDeployment:
    async def __call__(self, request: Request) -> str:
        body = await request.body()
        print("request.body()", body)
        return f"Hello {body} {time.time()}"


h = HttpDeployment.options(name="http-deployment").bind()
