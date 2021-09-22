from typing import Dict

import ray
from ray import serve
from ray.serve import pipeline

"""
This iteration uses a higher-level pipeline API to deploy a few different
models and send requests to them.

The models are deployed together and a single handle to them is retrieved.

Problems:
    - What if I need an escape hatch to inject custom business logic? I still
      want it deployed and versioned with the others.
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
    def __init__(self, pipeline_handle):
        self._pipeline_handle = pipeline_handle

    async def __call__(self, *args):
        return await self._pipeline_handle.remote()

# Deploy and get handle.
pipeline = pipeline.Sequential(
    preprocess,
    Model1.options(user_config="uri1.0"),
    Model2.options(user_config="uri2.0",
    name="my_pipeline", # Ugly, but needed for updates?
)
pipeline.deploy()

MyDriverDeployment.deploy(pipeline)

handle = MyDriverDeployment.get_handle()
output1 = ray.get(handle.remote())
assert output1 == "preprocess|Model1:uri1.0|Model2:uri2.0", output1

# Update URIs for model1 and model2:
pipeline = pipeline.Sequential(
    preprocess,
    Model1.options(user_config="uri1.1"),
    Model2.options(user_config="uri2.1"),
    name="my_pipeline", # Ugly, but needed for updates?
)
pipeline.deploy()

output2 = ray.get(handle.remote())
assert output2 == "preprocess|Model1:uri1.1|Model2:uri2.1", output2
