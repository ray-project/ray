import ray
from ray import serve
import logging
import requests

ray.init(address="auto")
client = serve.connect()

class Counter:
    def __init__(self):
        self.logger = serve.get_backend_logger()
        self.count = 0

    def __call__(self, request):
        self.count += 1
        self.logger.info(f"count: {self.count}")
        return {"count": self.count}

# Form a backend from our class and connect it to an endpoint.
client.create_backend("my_backend", Counter)
client.create_endpoint("my_endpoint", backend="my_backend", route="/counter")

# Query our endpoint in two different ways: from HTTP and from Python.
print(requests.get("http://127.0.0.1:8000/counter").json())