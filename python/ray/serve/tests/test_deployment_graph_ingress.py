import sys

import pytest

from ray import serve
from ray.serve._private.deployment_graph_build import build as pipeline_build
from ray.serve._private.deployment_graph_build import (
    get_and_validate_ingress_deployment,
)


@serve.deployment
class Model:
    def __init__(self, val):
        self.val = val

    def forward(self, input):
        return self.val + input


@serve.deployment
def func_deployment():
    return "hello"


@serve.deployment
def combine(input_1, input_2):
    return input_1 + input_2


@serve.deployment
class Driver:
    def __init__(self, dag):
        self.dag = dag

    async def __call__(self, inp):
        return await self.dag.remote(inp)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
