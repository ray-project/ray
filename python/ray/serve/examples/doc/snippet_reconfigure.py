import requests
import random

import ray
from ray import serve
from ray.serve import BackendConfig

ray.init()
serve.start()


class Threshold:
    def __init__(self):
        # self.model won't be changed by reconfigure.
        self.model = random.Random()  # Imagine this is some heavyweight model.

    def reconfigure(self, config):
        # This will be called when the class is created and when
        # the user_config field of BackendConfig is updated.
        self.threshold = config["threshold"]

    def __call__(self, request):
        return self.model.random() > self.threshold


backend_config = BackendConfig(user_config={"threshold": 0.01})
serve.create_backend("threshold", Threshold, config=backend_config)
serve.create_endpoint("threshold", backend="threshold", route="/threshold")
print(requests.get("http://127.0.0.1:8000/threshold").text)  # true, probably

backend_config = BackendConfig(user_config={"threshold": 0.99})
serve.update_backend_config("threshold", backend_config)
print(requests.get("http://127.0.0.1:8000/threshold").text)  # false, probably
