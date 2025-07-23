from enum import Enum

import starlette.requests

from ray import serve
from ray.serve.handle import DeploymentHandle


class Operation(str, Enum):
    ADD = "ADD"
    SUBTRACT = "SUB"


@serve.deployment(
    ray_actor_options={
        "num_cpus": 0.1,
    }
)
class Add:
    # Requires the test_dag repo as a py_module:
    # https://github.com/ray-project/test_dag

    def add(self, input: int) -> int:
        from dir2.library import add_one

        return add_one(input)


@serve.deployment(
    ray_actor_options={
        "num_cpus": 0.1,
    }
)
class Subtract:
    # Requires the test_module repo as a py_module:
    # https://github.com/ray-project/test_module

    def subtract(self, input: int) -> int:
        from test_module.test import one

        return input - one()  # Returns input - 2


@serve.deployment(
    ray_actor_options={
        "num_cpus": 0.1,
    }
)
class Router:
    def __init__(self, adder: DeploymentHandle, subtractor: DeploymentHandle):
        self.adder = adder
        self.subtractor = subtractor

    async def __call__(self, request: starlette.requests.Request) -> int:
        op, input = await request.json()

        if op == Operation.ADD:
            return await self.adder.add.remote(input)
        elif op == Operation.SUBTRACT:
            return await self.subtractor.subtract.remote(input)


adder = Add.bind()
subtractor = Subtract.bind()
serve_dag = Router.bind(adder, subtractor)
