from typing import Dict

import ray
from ray import serve
from ray.serve import pipeline

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
        self._pipeline = pipeline.Sequential(preprocess, Model1, Model2)
        preprocess.deploy()

    def reconfigure(self, uris: Dict[str, str]):
        Model1.options(user_config=uris["model1"]).deploy()
        Model2.options(user_config=uris["model2"]).deploy()

    async def __call__(self, *args):
        return await self._pipeline.remote()

MyDriverDeployment.options(
    user_config={"model1": "uri1.0", "model2": "uri2.0"}).deploy()
handle = MyDriverDeployment.get_handle()

output1 = ray.get(handle.remote())
assert output1 == "preprocess|Model1:uri1.0|Model2:uri2.0", output1

MyDriverDeployment.options(
    user_config={"model1": "uri1.1", "model2": "uri2.1"}).deploy()

output2 = ray.get(handle.remote())
assert output2 == "preprocess|Model1:uri1.1|Model2:uri2.1", output2
