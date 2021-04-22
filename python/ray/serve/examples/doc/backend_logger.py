import logging
import ray
from ray import serve
import requests

ray.init(address="auto")

logger = logging.getLogger("ray")


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, request):
        self.count += 1
        logger.info(f"count: {self.count}")
        return {"count": self.count}


serve.create_backend("my_backend", Counter)
serve.create_endpoint("my_endpoint", backend="my_backend", route="/counter")

for i in range(10):
    requests.get("http://127.0.0.1:8000/counter")
