# __start__
from ray import serve
from ray.util import metrics

import time
import requests


@serve.deployment
class MyDeployment:
    def __init__(self):
        self.num_requests = 0
        self.my_counter = metrics.Counter(
            "my_counter",
            description=("The number of odd-numbered requests to this deployment."),
            tag_keys=("deployment",),
        )
        self.my_counter.set_default_tags({"deployment": "MyDeployment"})

    def __call__(self):
        self.num_requests += 1
        if self.num_requests % 2 == 1:
            self.my_counter.inc()


my_deployment = MyDeployment.bind()
serve.run(my_deployment)

while True:
    requests.get("http://localhost:8000/")
    time.sleep(1)

    # __end__
    break

response = requests.get("http://localhost:8000/")
assert response.status_code == 200
