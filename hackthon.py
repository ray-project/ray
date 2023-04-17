import logging
import random
import time

import requests
import starlette
from fastapi import FastAPI

from ray import serve

logger = logging.getLogger(__file__)



@serve.deployment
@serve.multiplex(num_models_per_replicas=3)
class ModelTest:
    def __init__(self, tag: str):
        self.tag = tag

    async def __call__(self, request: starlette.requests.Request):
        time.sleep(1)
        return f"hello {self.tag}"


handle = serve.run(ModelTest.bind())
headers = {"ray_serve_request_routing_tag": str(1)}
resp = requests.get(
    "http://127.0.0.1:8000/run", headers=headers
)
#import pdb; pdb.set_trace()
#with request:
#    handle.remote()


##############################
"""
app = FastAPI()


@serve.deployment(num_replicas=3)
@serve.multiplex(num_models_per_replicas=3)
@serve.ingress(app)
class ModelMultiplex:
    def __init__(self, tag: str):
        self.tag = tag
        self._model = None
        logger.info(f"loading '{tag}' model")

    @app.get("/run")
    async def run(self, request: starlette.requests.Request):
        time.sleep(1)
        return f"[ModelMultiplex] hello run {self.tag}"
        # return self._model(*args, **kwargs)

    @app.post("/run2")
    async def run2(self, request: starlette.requests.Request):
        time.sleep(1)
        return f"[ModelMultiplex] post hello run2 {self.tag}"


app2 = FastAPI()


@serve.deployment(num_replicas=3)
@serve.multiplex(num_models_per_replicas=3)
@serve.ingress(app2)
class ModelMultiplex2:
    def __init__(self, tag: str):
        self.tag = tag
        self._model = None
        logger.info(f"loading '{tag}' model")

    @app2.get("/run")
    async def run(self, request: starlette.requests.Request):
        time.sleep(1)
        return f"[ModelMultiplex2] hello run {self.tag}"
        # return self._model(*args, **kwargs)

    @app2.post("/run2")
    async def run2(self, request: starlette.requests.Request):
        time.sleep(1)
        return f"[ModelMultiplex2] post hello run2 {self.tag}"


serve.run(ModelMultiplex.bind(), name="app", route_prefix="/app")
serve.run(ModelMultiplex2.bind(), name="app2", route_prefix="/app2")


# time.sleep(3600)
num_unload = {}

for i in range(1000):
    model_id = random.randint(0, 9)
    headers = {"ray_serve_request_routing_tag": str(model_id)}
    get_urls = ["http://127.0.0.1:8000/app/run", "http://127.0.0.1:8000/app2/run"]
    post_urls = ["http://127.0.0.1:8000/app/run2", "http://127.0.0.1:8000/app2/run2"]
    for url in get_urls:
        resp = requests.get(url, headers=headers)
        print(resp.text)
    for url in post_urls:
        resp = requests.post(url, headers=headers)
        print(resp.text)
    # resp = requests.get(
    #    "http://127.0.0.1:8000/app/run2", headers=headers
    # )
    # print(resp)
    # resp = resp.json()
"""

# assert len(num_unload) == 6
# print("Unload metrics: ", sum(num_unload.values()))
