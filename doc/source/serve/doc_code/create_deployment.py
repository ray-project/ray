# flake8: noqa
# fmt: off
#
# __serve_example_begin__
#
# This brief example shows how to create, deploy, and expose access to
# deployment models, using the simple Ray Serve deployment APIs.
# Once deployed, you can access deployment via two methods:
# ServerHandle API and HTTP
#
import os
from random import random

import requests
import starlette.requests
from ray import serve

#
# A simple example model stored in a pickled format at an accessible path
# that can be reloaded and deserialized into a model instance. Once deployed
# in Ray Serve, we can use it for prediction. The prediction is a fake condition,
# based on threshold of weight greater than 0.5.
#


class Model:
    def __init__(self, path):
        self.path = path

    def predict(self, data: float) -> float:
        return random() + data if data > 0.5 else data


@serve.deployment
class Predictor:
    # Take in a path to load your desired model
    def __init__(self, path: str) -> None:
        self.path = path
        self.model = Model(path)
        # Get the pid on which this deployment is running on
        self.pid = os.getpid()

    # Deployments are callable. Here we simply return a prediction from
    # our request.
    async def predict(self, data: float) -> str:
        pred = self.model.predict(data)
        return (f"(pid: {self.pid}); path: {self.path}; "
                f"data: {float(data):.3f}; prediction: {pred:.3f}")

    async def __call__(self, http_request: starlette.requests.Request) -> str:
        data = float(await http_request.query_params['data'])
        return await self.predict(data)


@serve.deployment
class ServeHandleDemo:
    def __init__(self, predictor_1: Predictor, predictor_2: Predictor):
        self.predictor_1 = predictor_1
        self.predictor_2 = predictor_2

    async def run(self):
        # Query each deployment twice to demonstrate that the requests
        # get forwarded to different replicas (below, we will set
        # num_replicas to 2 for each deployment).
        for _ in range(2):
            for predictor in [self.predictor_1, self.predictor_2]:
                # Call our deployments from Python using the ServeHandle API.
                random_prediction = await predictor.predict.remote(random())
                print(f"prediction: {random_prediction}")

    async def __call__(self, http_request: starlette.requests.Request) -> str:
        return await self.run()


predictor_1 = Predictor.options(num_replicas=2).bind("/model/model-1.pkl")
predictor_2 = Predictor.options(num_replicas=2).bind("/model/model-2.pkl")

# Pass in our deployments as arguments.  At runtime, these are resolved to ServeHandles.
serve_handle_demo = ServeHandleDemo.bind(predictor_1, predictor_2)

# Start a local single-node Ray cluster and start Ray Serve. These will shut down upon
# exiting this script. 
serve.run(serve_handle_demo)

print("ServeHandle API responses: " + "--" * 5)

url = "http://127.0.0.1:8000/"
response = requests.get(url)
prediction = response.text
print(f"prediction : {prediction}")

# Output ("INFO" logs omitted for brevity):

# (ServeReplica:ServeHandleDemo pid=16062) prediction: (pid: 16059); path: /model/model-1.pkl; data: 0.166; prediction: 0.166
# (ServeReplica:ServeHandleDemo pid=16062) prediction: (pid: 16061); path: /model/model-2.pkl; data: 0.820; prediction: 0.986
# (ServeReplica:ServeHandleDemo pid=16062) prediction: (pid: 16058); path: /model/model-1.pkl; data: 0.691; prediction: 0.857
# (ServeReplica:ServeHandleDemo pid=16062) prediction: (pid: 16060); path: /model/model-2.pkl; data: 0.948; prediction: 1.113
# __serve_example_end__
