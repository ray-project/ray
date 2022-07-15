import requests

import ray
from ray import serve

serve.start()


@serve.deployment
class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, *args):
        self.count += 1
        return {"count": self.count}


# Deploy our class.
Counter.deploy()

# Query our endpoint in two different ways: from HTTP and from Python.
assert requests.get("http://127.0.0.1:8000/Counter").json() == {"count": 1}
assert ray.get(Counter.get_handle().remote()) == {"count": 2}
