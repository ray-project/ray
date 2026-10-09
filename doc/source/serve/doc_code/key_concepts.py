# flake8: noqa

# __start_my_first_deployment__
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class MyFirstDeployment:
    # Take the message to return as an argument to the constructor.
    def __init__(self, msg):
        self.msg = msg

    def __call__(self):
        return self.msg


my_first_deployment = MyFirstDeployment.bind("Hello world!")
handle: DeploymentHandle = serve.run(my_first_deployment)
assert handle.remote().result() == "Hello world!"
# __end_my_first_deployment__

# __start_deployment_handle__
from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class Hello:
    def __call__(self) -> str:
        return "Hello"


@serve.deployment
class World:
    def __call__(self) -> str:
        return " world!"


@serve.deployment
class Ingress:
    def __init__(self, hello_handle: DeploymentHandle, world_handle: DeploymentHandle):
        self._hello_handle = hello_handle
        self._world_handle = world_handle

    async def __call__(self) -> str:
        hello_response = self._hello_handle.remote()
        world_response = self._world_handle.remote()
        return (await hello_response) + (await world_response)


hello = Hello.bind()
world = World.bind()

# The deployments passed to the Ingress constructor are replaced with handles.
app = Ingress.bind(hello, world)

# Deploys Hello, World, and Ingress.
handle: DeploymentHandle = serve.run(app)

# `DeploymentHandle`s can also be used to call the ingress deployment of an application.
assert handle.remote().result() == "Hello world!"
# __end_deployment_handle__

# __start_basic_ingress__
import requests
from starlette.requests import Request

from ray import serve


@serve.deployment
class MostBasicIngress:
    async def __call__(self, request: Request) -> str:
        name = (await request.json())["name"]
        return f"Hello {name}!"


app = MostBasicIngress.bind()
serve.run(app)
assert (
    requests.get("http://127.0.0.1:8000/", json={"name": "Corey"}).text
    == "Hello Corey!"
)
# __end_basic_ingress__

# __start_fastapi_ingress__
import requests
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from ray import serve

fastapi_app = FastAPI()


@serve.deployment
@serve.ingress(fastapi_app)
class FastAPIIngress:
    @fastapi_app.get("/{name}")
    async def say_hi(self, name: str) -> str:
        return PlainTextResponse(f"Hello {name}!")


app = FastAPIIngress.bind()
serve.run(app)
assert requests.get("http://127.0.0.1:8000/Corey").text == "Hello Corey!"
# __end_fastapi_ingress__
