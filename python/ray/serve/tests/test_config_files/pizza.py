from enum import Enum
from typing import List, Dict, TypeVar

from ray import serve
from ray.serve.handle import RayServeDeploymentHandle
import starlette.requests
from ray.serve.drivers import DAGDriver
from ray.serve.deployment_graph import InputNode

RayHandleLike = TypeVar("RayHandleLike")


class Operation(str, Enum):
    ADDITION = "ADD"
    MULTIPLICATION = "MUL"


@serve.deployment(ray_actor_options={"num_cpus": 0.15})
class Router:
    def __init__(
        self, multiplier: RayServeDeploymentHandle, adder: RayServeDeploymentHandle
    ):
        self.adder = adder
        self.multiplier = multiplier

    async def route(self, op: Operation, input: int) -> int:
        if op == Operation.ADDITION:
            return await (await self.adder.add.remote(input))
        elif op == Operation.MULTIPLICATION:
            return await (await self.multiplier.multiply.remote(input))


@serve.deployment(
    user_config={
        "factor": 3,
    },
    ray_actor_options={"num_cpus": 0.15},
)
class Multiplier:
    def __init__(self, factor: int):
        self.factor = factor

    def reconfigure(self, config: Dict):
        self.factor = config.get("factor", -1)

    def multiply(self, input_factor: int) -> int:
        return input_factor * self.factor


@serve.deployment(
    user_config={
        "increment": 2,
    },
    ray_actor_options={"num_cpus": 0.15},
)
class Adder:
    def __init__(self, increment: int):
        self.increment = increment

    def reconfigure(self, config: Dict):
        self.increment = config.get("increment", -1)

    def add(self, input: int) -> int:
        return input + self.increment


@serve.deployment(ray_actor_options={"num_cpus": 0.15})
def create_order(amount: int) -> str:
    return f"{amount} pizzas please!"


async def json_resolver(request: starlette.requests.Request) -> List:
    return await request.json()


# Overwritten by user_config
ORIGINAL_INCREMENT = 1
ORIGINAL_FACTOR = 1

with InputNode() as inp:
    operation, amount_input = inp[0], inp[1]

    multiplier = Multiplier.bind(ORIGINAL_FACTOR)
    adder = Adder.bind(ORIGINAL_INCREMENT)
    router = Router.bind(multiplier, adder)
    amount = router.route.bind(operation, amount_input)
    order = create_order.bind(amount)

serve_dag = DAGDriver.bind(order, http_adapter=json_resolver)
