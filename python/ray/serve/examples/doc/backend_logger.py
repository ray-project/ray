import ray
from ray import serve
import logging
import requests

ray.init()
client = serve.start()

class Counter:
    def __init__(self):
        self.logger = serve.get_backend_logger()
        self.count = 0

    def __call__(self, request):
        self.count += 1
        self.logger.info(f"count: {self.count}")
        return {"count": self.count}

client.create_backend("my_backend", Counter)
client.create_endpoint("my_endpoint", backend="my_backend", route="/counter")

for i in range(10):
    requests.get("http://127.0.0.1:8000/counter")