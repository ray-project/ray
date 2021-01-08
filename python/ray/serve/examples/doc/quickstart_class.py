import ray
from ray import serve
import requests

ray.init(num_cpus=4)
client = serve.start()


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, request):
        self.count += 1
        return {"count": self.count}


# Form a backend from our class and connect it to an endpoint.
client.create_backend("my_backend", Counter)
client.create_endpoint("my_endpoint", backend="my_backend", route="/counter")

# Query our endpoint in two different ways: from HTTP and from Python.
print(requests.get("http://127.0.0.1:8000/counter").json())
# > {"count": 1}
print(ray.get(client.get_handle("my_endpoint").remote()))
# > {"count": 2}
