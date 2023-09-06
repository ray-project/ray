import requests

# __serve_example_begin__
from starlette.requests import Request

from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class Adder:
    def __call__(self, val: int) -> int:
        return val + 2


@serve.deployment
class Multiplier:
    def __call__(self, val: int) -> int:
        return val * 2


@serve.deployment
class Ingress:
    def __init__(self, adder, multiplier):
        self.adder: DeploymentHandle = adder.options(use_new_handle_api=True)
        self.multiplier: DeploymentHandle = multiplier.options(use_new_handle_api=True)

    async def __call__(self, request: Request) -> int:
        input = await request.json()

        if input["op"] == "ADD":
            return await self.adder.remote(input["val"])
        elif input["op"] == "MUL":
            return await self.multiplier.remote(input["val"])


app = Ingress.bind(Adder.bind(), Multiplier.bind())
# __serve_example_end__

if __name__ == "__main__":
    serve.run(app)
    resp = requests.post(
        "http://localhost:8000/", json={"op": "ADD", "val": 5}
    ).json()
    assert resp == 7, resp
