import logging
from ray import serve
import requests

logger = logging.getLogger("ray.serve")


@serve.deployment
class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, request):
        self.count += 1
        logger.info(f"count: {self.count}")
        return {"count": self.count}


counter = Counter.bind()
serve.run(counter)

for i in range(10):
    requests.get("http://127.0.0.1:8000/")
