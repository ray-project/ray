# flake8: noqa
import ray
from ray import serve


# __my_first_deployment_start__
@serve.deployment
class MyFirstDeployment:
    # Take the message to return as an argument to the constructor.
    def __init__(self, msg):
        self.msg = msg

    def __call__(self):
        return self.msg


my_first_deployment = MyFirstDeployment.bind("Hello world!")
handle = serve.run(my_first_deployment)
print(ray.get(handle.remote()))  # "Hello world!"
# __my_first_deployment_end__


@serve.deployment
class ModelA:
    def __call__(self):
        return "model_a"


@serve.deployment
class ModelB:
    def __call__(self):
        return "model_b"


# __driver_start__
@serve.deployment
class Driver:
    def __init__(self, model_a_handle, model_b_handle):
        self._model_a_handle = model_a_handle
        self._model_b_handle = model_b_handle

    async def __call__(self, request):
        ref_a = await self._model_a_handle.remote(request)
        ref_b = await self._model_b_handle.remote(request)
        return (await ref_a) + (await ref_b)


model_a = ModelA.bind()
model_b = ModelB.bind()

# model_a and model_b will be passed to the Driver constructor as ServeHandles.
driver = Driver.bind(model_a, model_b)

# Deploys model_a, model_b, and driver.
serve.run(driver)
# __driver_end__

import starlette


# __most_basic_ingress_start__
@serve.deployment
class MostBasicIngress:
    async def __call__(self, request: starlette.requests.Request) -> str:
        name = await request.json()["name"]
        return f"Hello {name}"
# __most_basic_ingress_end__


# __request_get_start__
import requests
print(requests.get("http://127.0.0.1:8000/", json={"name": "Corey"}).text)  # Hello Corey!
# __request_get_end__


# __fastapi_start__
from fastapi import FastAPI

app = FastAPI()


@serve.deployment
@serve.ingress(app)
class MostBasicIngress:
    @app.get("/{name}")
    async def say_hi(self, name: str) -> str:
        return f"Hello {name}"
# __fastapi_end__
