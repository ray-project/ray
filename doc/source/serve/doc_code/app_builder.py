# flake8: noqa

# __begin_untyped_builder__
# hello.py

from typing import Dict

from ray import serve
from ray.serve import Application


@serve.deployment
class HelloWorld:
    def __init__(self, message: str):
        self._message = message
        print("Message:", self._message)

    def __call__(self, request):
        return self._message


def app_builder(args: Dict[str, str]) -> Application:
    return HelloWorld.bind(args["message"])


# __end_untyped_builder__

import requests

serve.run(app_builder({"message": "Hello bar"}))
resp = requests.get("http://localhost:8000")
assert resp.text == "Hello bar"

# __begin_typed_builder__
# hello.py

from pydantic import BaseModel

from ray import serve
from ray.serve import Application


class HelloWorldArgs(BaseModel):
    message: str


@serve.deployment
class HelloWorld:
    def __init__(self, message: str):
        self._message = message
        print("Message:", self._message)

    def __call__(self, request):
        return self._message


def typed_app_builder(args: HelloWorldArgs) -> Application:
    return HelloWorld.bind(args.message)


# __end_typed_builder__

serve.run(typed_app_builder(HelloWorldArgs(message="Hello baz")))
resp = requests.get("http://localhost:8000")
assert resp.text == "Hello baz"

# __begin_composed_builder__
# hello.py
from pydantic import BaseModel
from starlette.requests import Request

from ray import serve
from ray.serve import Application
from ray.serve.handle import DeploymentHandle


class ComposedArgs(BaseModel):
    increment: int
    multiplier: int


@serve.deployment
class Adder:
    def __init__(self, increment: int):
        self._increment = increment

    def __call__(self, value: int) -> int:
        return value + self._increment


@serve.deployment
class Multiplier:
    def __init__(self, multiplier: int):
        self._multiplier = multiplier

    def __call__(self, value: int) -> int:
        return value * self._multiplier


@serve.deployment
class IngressDeployment:
    def __init__(self, adder: DeploymentHandle, multiplier: DeploymentHandle):
        self._adder = adder
        self._multiplier = multiplier

    async def __call__(self, request: Request) -> int:
        value = int(request.query_params["value"])
        added = await self._adder.remote(value)
        return await self._multiplier.remote(added)


def composed_app_builder(args: ComposedArgs) -> Application:
    return IngressDeployment.bind(
        Adder.bind(args.increment),
        Multiplier.bind(args.multiplier),
    )


# __end_composed_builder__

for args, expected in [
    (ComposedArgs(increment=1, multiplier=2), 12),
    (ComposedArgs(increment=3, multiplier=4), 32),
]:
    serve.run(composed_app_builder(args))
    response = requests.get("http://localhost:8000", params={"value": 5}, timeout=30)
    response.raise_for_status()
    assert response.json() == expected
