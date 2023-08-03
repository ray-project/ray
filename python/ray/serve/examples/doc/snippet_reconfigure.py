import requests
import random

from ray import serve

serve.start()


@serve.deployment(route_prefix="/threshold")
class Threshold:
    def __init__(self):
        # self.model won't be changed by reconfigure.
        self.model = random.Random()  # Imagine this is some heavyweight model.

    def reconfigure(self, config):
        # This will be called when the class is created and when
        # the user_config is updated.
        self.threshold = config["threshold"]

    def __call__(self, request):
        return self.model.random() > self.threshold


Threshold.options(user_config={"threshold": 0.01}).deploy()
print(requests.get("http://127.0.0.1:8000/threshold").text)  # true, probably

Threshold.options(user_config={"threshold": 0.99}).deploy()
print(requests.get("http://127.0.0.1:8000/threshold").text)  # false, probably
