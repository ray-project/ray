import requests

# __serve_example_begin__
from ray import serve
from ray.serve.handle import RayServeHandle
from ray.serve.drivers import DAGDriver
from ray.serve.deployment_graph import InputNode
from ray.serve.http_adapters import json_request


@serve.deployment
class Adder:
    def __call__(self, input: int) -> int:
        return input + 2


@serve.deployment
class Multiplier:
    def __call__(self, input: int) -> int:
        return input * 2


@serve.deployment
class Router:
    def __init__(
        self,
        adder: RayServeHandle,
        multiplier: RayServeHandle,
    ):
        self.adder = adder
        self.multiplier = multiplier

    async def route(self, op: str, input: int) -> int:
        if op == "ADD":
            return await (await self.adder.remote(input))
        elif op == "MUL":
            return await (await self.multiplier.remote(input))


with InputNode() as inp:
    operation, amount_input = inp[0], inp[1]

    multiplier = Multiplier.bind()
    adder = Adder.bind()
    router = Router.bind(adder, multiplier)
    amount = router.route.bind(operation, amount_input)

app = DAGDriver.options(route_prefix="/calculator").bind(
    amount, http_adapter=json_request
)
# __serve_example_end__

# Test
if __name__ == "__main__":
    serve.run(app)
    resp = requests.post("http://localhost:8000/calculator", json=["ADD", 5]).json()
    assert resp == 7
