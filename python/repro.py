import time
import requests
import ray
from ray.util.metrics import Histogram

ray.init(_metrics_export_port=8080)

@ray.remote
class MyActor:
    def __init__(self, name):
        self.histogram = Histogram(
            "request_latency",
            description="Latencies of requests in ms.",
            boundaries=[0, 1, 2, 3],
        )

    def process_request(self, num):
        self.histogram.observe(num)


my_actor = MyActor.remote("my_actor")
ray.get(my_actor.process_request.remote(observation))
ray.get(my_actor.process_request.remote(observation))

for i in range(30):
    time.sleep(1)
    text = requests.get("http://localhost:8080").text
    if 'ray_request_latency_sum' not in text:
        print("Metric not found, retrying!")
    else:
        print("Metric found!")
        break

print("Exiting!")
