# __serve_example_begin__
import time

from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class LightLoad:
    async def __call__(self) -> str:
        start = time.time()
        while time.time() - start < 0.1:
            pass

        return "light"


@serve.deployment
class HeavyLoad:
    async def __call__(self) -> str:
        start = time.time()
        while time.time() - start < 0.2:
            pass

        return "heavy"


@serve.deployment
class Driver:
    def __init__(self, a_handle, b_handle):
        self.a_handle: DeploymentHandle = a_handle
        self.b_handle: DeploymentHandle = b_handle

    async def __call__(self) -> str:
        a_future = self.a_handle.remote()
        b_future = self.b_handle.remote()

        return (await a_future), (await b_future)


app = Driver.bind(HeavyLoad.bind(), LightLoad.bind())
# __serve_example_end__

import requests  # noqa

serve.run(app)
resp = requests.post("http://localhost:8000")
assert resp.json() == ["heavy", "light"]
