from typing import Dict

import ray
from ray import serve
from ray.serve import pipeline

"""
This iteration introduces a new concept: a DeploymentGroup, which is a group
of deployments that are all deployed, versioned, and upgraded together.

The models are deployed together in the DeploymentGroup and then they are
combined into a pipeline and a single handle to them is retrieved.

Problems:
    - This API doesn't look very pretty or easy to understand :(
"""

serve.start()

@serve.deployment(route_prefix=None)
def preprocess(*args):
    return "preprocess"

@serve.deployment(route_prefix=None)
class Model1:
    def reconfigure(self, uri: str):
        self._uri = uri

    def __call__(self, prev: str):
        return prev + f"|Model1:{self._uri}"

@serve.deployment(route_prefix=None)
class Model2:
    def reconfigure(self, uri: str):
        self._uri = uri

    def __call__(self, prev: str):
        return prev + f"|Model2:{self._uri}"

@serve.deployment(route_prefix="/pipeline")
class MyDriverDeployment:
    def __init__(self, pipeline_handle):
        self._pipeline_handle = pipeline_handle

    async def __call__(self, *args):
        return await self._pipeline_handle.remote()

# Deploy and get handle.
with serve.DeploymentGroup("my_pipeline", version="frozen"):
    preprocess.deploy()
    Model1.options(user_config="uri1.0").deploy()
    Model2.options(user_config="uri2.0").deploy()

    pipeline = pipeline.Sequential(
        preprocess,
        Model1,
        Model2,
    )

    MyDriverDeployment.deploy(pipeline)

handle = MyDriverDeployment.get_handle()
output1 = ray.get(handle.remote())
assert output1 == "preprocess|Model1:uri1.0|Model2:uri2.0", output1

# Update URIs for model1 and model2:
with serve.DeploymentGroup("my_pipeline", version="frozen"):
    preprocess.deploy()
    Model1.options(user_config="uri1.1").deploy()
    Model2.options(user_config="uri2.1").deploy()

    pipeline = pipeline.Sequential(
        preprocess,
        Model1,
        Model2,
    )

    MyDriverDeployment.deploy(pipeline)

output2 = ray.get(handle.remote())
assert output2 == "preprocess|Model1:uri1.1|Model2:uri2.1", output2
