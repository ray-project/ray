import ray
from ray import serve
import requests

ray.init(num_cpus=8)
client = serve.start()


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, flask_request):
        return {"current_counter": self.count}


client.create_backend("counter", Counter)
client.create_endpoint("counter", backend="counter", route="/counter")

requests.get("http://127.0.0.1:8000/counter").json()
# > {"current_counter": self.count}
