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
import ray
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
        for i in range(2):
            for predictor in [self.predictor_1, self.predictor_2]:
                # Call our deployments from Python using the ServeHandle API.
                random_prediction = await predictor.predict.remote(random())
                print(f"prediction: {random_prediction}")

    async def __call__(self, http_request: starlette.requests.Request) -> str:
        return await self.run()


rep_1_predictor = Predictor.options(
    name="rep_1", num_replicas=2).bind("/model/rep-1.pkl")
rep_2_predictor = Predictor.options(
    name="rep_2", num_replicas=2).bind("/model/rep-2.pkl")

# Pass in our deployments as arguments.  At runtime, these are resolved to ServeHandles.
serve_handle_demo = ServeHandleDemo.bind(rep_1_predictor, rep_2_predictor)

serve.run(serve_handle_demo)

print("ServeHandle API responses: " + "--" * 5)

url = f"http://127.0.0.1:8000/"
response = requests.get("http://127.0.0.1:8000/")
prediction = response.text
print(f"prediction : {prediction}")

# Output (info logs omitted for brevity):

# (ServeReplica:ServeHandleDemo pid=92100) prediction: (pid: 92096); path: /model/rep-1.pkl; data: 0.721; prediction: 1.442
# (ServeReplica:ServeHandleDemo pid=92100) prediction: (pid: 92099); path: /model/rep-2.pkl; data: 0.204; prediction: 0.204
# (ServeReplica:ServeHandleDemo pid=92100) prediction: (pid: 92097); path: /model/rep-1.pkl; data: 0.669; prediction: 1.390
# (ServeReplica:ServeHandleDemo pid=92100) prediction: (pid: 92098); path: /model/rep-2.pkl; data: 0.791; prediction: 1.511
# __serve_example_end__
