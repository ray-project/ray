# flake8: noqa
import ray


ray.init()


# __begin_start_grpc_proxy__
from ray import serve
from ray.serve.config import gRPCOptions


grpc_port = 9000
grpc_servicer_functions = [
    "user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server",
    "user_defined_protos_pb2_grpc.add_FruitServiceServicer_to_server",
]
serve.start(
    http_options={"location": "EveryNode"},
    grpc_options=gRPCOptions(
        port=grpc_port,
        grpc_servicer_functions=grpc_servicer_functions,
    ),
)
# __end_start_grpc_proxy__

# __begin_grpc_deployment__
import time

from typing import Dict, Generator
from user_defined_protos_pb2 import (
    FruitAmounts,
    FruitCosts,
    UserDefinedMessage,
    UserDefinedMessage2,
    UserDefinedResponse,
    UserDefinedResponse2,
)

import ray
from ray import serve
from ray.serve.handle import RayServeDeploymentHandle


@serve.deployment
class GrpcDeployment:
    def __call__(self, user_message: UserDefinedMessage) -> UserDefinedResponse:
        greeting = f"Hello {user_message.name} from {user_message.foo}"
        num_x2 = user_message.num * 2
        user_response = UserDefinedResponse(
            greeting=greeting,
            num_x2=num_x2,
        )
        return user_response

    def Method1(self, user_message: UserDefinedMessage) -> UserDefinedResponse:
        greeting = f"Hello {user_message.foo} from method1"
        num_x2 = user_message.num * 3
        user_response = UserDefinedResponse(
            greeting=greeting,
            num_x2=num_x2,
        )
        return user_response

    @serve.multiplexed(max_num_models_per_replica=1)
    async def get_model(self, model_id: str) -> str:
        return f"loading model: {model_id}"

    async def Method2(self, user_message):
        model_id = serve.get_multiplexed_model_id()
        model = await self.get_model(model_id)
        user_response = serve_pb2.UserDefinedResponse(
            greeting=f"Method2 called model, {model}",
        )
        return user_response

    def Streaming(
        self, user_message: UserDefinedMessage
    ) -> Generator[UserDefinedResponse, None, None]:
        for i in range(10):
            greeting = f"{i}: Hello {user_message.name} from {user_message.foo}"
            num_x2 = user_message.num * 2 + i
            user_response = UserDefinedResponse(
                greeting=greeting,
                num_x2=num_x2,
            )
            yield user_response

            time.sleep(0.1)


g = GrpcDeployment.options(name="app1").bind()

# __begin_grpc_deployment__


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

    async def FruitStand(self, fruit_amounts_proto: FruitAmounts) -> FruitCosts:
        fruit_amounts = {}
        if fruit_amounts_proto.orange:
            fruit_amounts["ORANGE"] = fruit_amounts_proto.orange
        if fruit_amounts_proto.apple:
            fruit_amounts["APPLE"] = fruit_amounts_proto.apple
        if fruit_amounts_proto.banana:
            fruit_amounts["BANANA"] = fruit_amounts_proto.banana

        costs = await self.check_price(fruit_amounts)
        return FruitCosts(costs=costs)

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
g2 = FruitMarket.options(name="grpc-deployment-model-composition").bind(
    orange_stand, apple_stand
)
