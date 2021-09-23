from typing import Dict

import ray
from ray import serve
"""
This iteration uses the existing APIs in Ray Serve.

It deploys a few models as independent deployments and then retrieves a handle
to each of them to send requests to.

Problems:
    - The user needs to write low-level asyncio code to forward requests between the models.
    - The deployments are independently deployed, versioned, and updated.
"""

serve.start()


@serve.deployment(route_prefix=None, version="frozen")
def preprocess(*args):
    return "preprocess"


@serve.deployment(route_prefix=None, version="frozen")
class Model1:
    def reconfigure(self, uri: str):
        self._uri = uri

    def __call__(self, prev: str):
        return prev + f"|Model1:{self._uri}"


@serve.deployment(route_prefix=None, version="frozen")
class Model2:
    def reconfigure(self, uri: str):
        self._uri = uri

    def __call__(self, prev: str):
        return prev + f"|Model2:{self._uri}"


@serve.deployment(route_prefix="/pipeline", version="1")
class MyDriverDeployment:
    def __init__(self):
        self.preprocessing_handle = preprocess.get_handle(sync=False)
        self.model_1_handle = Model1.get_handle(sync=False)
        self.model_2_handle = Model2.get_handle(sync=False)

    async def __call__(self, *args):
        ref1 = await self.preprocessing_handle.remote()
        ref2 = await self.model_1_handle.remote(ref1)
        ref3 = await self.model_2_handle.remote(ref2)
        return await ref3


# Deploy and get handle.
preprocess.deploy()
Model1.options(user_config="uri1.0").deploy()
Model2.options(user_config="uri2.0").deploy()

MyDriverDeployment.deploy()

handle = MyDriverDeployment.get_handle()
output1 = ray.get(handle.remote())
assert output1 == "preprocess|Model1:uri1.0|Model2:uri2.0", output1

# Update URIs for model1 and model2:
Model1.options(user_config="uri1.1").deploy()
Model2.options(user_config="uri2.1").deploy()

output2 = ray.get(handle.remote())
assert output2 == "preprocess|Model1:uri1.1|Model2:uri2.1", output2
