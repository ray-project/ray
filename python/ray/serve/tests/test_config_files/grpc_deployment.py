# Users need to include their custom message type which will be embedded in the request.
import ray
from ray.serve.generated import serve_pb2

from ray import serve
from ray.serve.handle import RayServeDeploymentHandle
from typing import Dict


@serve.deployment
class GrpcDeployment:
    def __call__(self, user_message):
        greeting = f"Hello {user_message.name} from {user_message.foo}"
        num_x2 = user_message.num * 2
        user_response = serve_pb2.UserDefinedResponse(
            greeting=greeting,
            num_x2=num_x2,
        )
        return user_response

    def Method1(self, user_message):
        greeting = f"Hello {user_message.name} from method1"
        num_x2 = user_message.num * 3
        user_response = serve_pb2.UserDefinedResponse(
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

    def Streaming(self, user_message):
        for i in range(10):
            greeting = f"{i}: Hello {user_message.name} from {user_message.foo}"
            num_x2 = user_message.num * 2 + i
            user_response = serve_pb2.UserDefinedResponse(
                greeting=greeting,
                num_x2=num_x2,
            )
            yield user_response


g = GrpcDeployment.options(name="grpc-deployment").bind()


@serve.deployment(ray_actor_options={"num_cpus": 0})
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

    async def FruitStand(self, fruit_amounts_proto):
        fruit_amounts = {}
        if fruit_amounts_proto.orange:
            fruit_amounts["ORANGE"] = fruit_amounts_proto.orange
        if fruit_amounts_proto.apple:
            fruit_amounts["APPLE"] = fruit_amounts_proto.apple
        if fruit_amounts_proto.banana:
            fruit_amounts["BANANA"] = fruit_amounts_proto.banana

        costs = await self.check_price(fruit_amounts)
        return serve_pb2.FruitCosts(costs=costs)

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


@serve.deployment(ray_actor_options={"num_cpus": 0})
class OrangeStand:
    def __init__(self):
        self.price = 2.0

    def __call__(self, num_oranges: int):
        return num_oranges * self.price


@serve.deployment(ray_actor_options={"num_cpus": 0})
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
