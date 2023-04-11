import logging
import random
import time

import requests
import starlette

from ray import serve

logger = logging.getLogger(__file__)


@serve.deployment(num_replicas=6)
@serve.multiplex(num_models_per_replicas=3)
class ModelMultiplex:
    def __init__(self, tag: str):
        self.tag = tag
        self._model = None
        logger.info(f"loading '{tag}' model")

    async def __call__(self, request: starlette.requests.Request):
        time.sleep(1)
        return f"hello {self.tag}"
        # return self._model(*args, **kwargs)


serve.run(ModelMultiplex.bind())

# time.sleep(3600)
num_unload = {}

for i in range(1000):
    model_id = random.randint(0, 15)
    headers = {"ray_serve_request_routing_tag": str(model_id)}
    resp = requests.get(
        "http://127.0.0.1:8000", headers=headers
    )
    resp = resp.json()
    num_unload[resp[0]] = resp[1]

assert len(num_unload) == 6
print("Unload metrics: ", sum(num_unload.values()))
