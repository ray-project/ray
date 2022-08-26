# __local_dev_start__
# Filename: local_dev.py
import ray
from ray import serve
import starlette.requests


@serve.deployment
class Doubler:
    def double(self, s: str):
        return s + " " + s


@serve.deployment
class HelloDeployment:
    def __init__(self, doubler):
        self.doubler = doubler

    async def say_hello_twice(self, name: str):
        ref = await self.doubler.double.remote(f"Hello, {name}!")
        return await ref

    async def __call__(self, request: starlette.requests.Request):
        return await self.say_hello_twice(request.query_params["name"])


graph = HelloDeployment.bind(Doubler.bind())
# __local_dev_end__

# __local_dev_handle_start__
handle = serve.run(graph)
result = ray.get(handle.say_hello_twice.remote(name="Ray"))
assert result == "Hello, Ray! Hello, Ray!"
# __local_dev_handle_end__
