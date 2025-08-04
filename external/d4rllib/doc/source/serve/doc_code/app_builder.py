# flake8: noqa

# __begin_untyped_builder__
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
from pydantic import BaseModel

from ray.serve import Application


class ComposedArgs(BaseModel):
    model1_uri: str
    model2_uri: str


def composed_app_builder(args: ComposedArgs) -> Application:
    return IngressDeployment.bind(
        Model1.bind(args.model1_uri),
        Model2.bind(args.model2_uri),
    )


# __end_composed_builder__
