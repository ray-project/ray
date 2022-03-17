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
import starlette
from starlette.requests import Request
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

    def predict(self, data):
        return random() + data if data > 0.5 else data


@serve.deployment
class Deployment:
    # Take in a path to load your desired model
    def __init__(self, path: str) -> None:
        self.path = path
        self.model = Model(path)
        # Get the pid on which this deployment is running on
        self.pid = os.getpid()

    # Deployments are callable. Here we simply return a prediction from
    # our request
    def __call__(self, starlette_request) -> str:
        # Request came via an HTTP
        if isinstance(starlette_request, starlette.requests.Request):
            data = starlette_request.query_params['data']
        else:
            # Request came via a ServerHandle API method call.
            data = starlette_request
        pred = self.model.predict(float(data))
        return f"(pid: {self.pid}); path: {self.path}; data: {float(data):.3f}; prediction: {pred:.3f}"


if __name__ == '__main__':

    # Start a Ray Serve instance. This will automatically start
    # or connect to an existing Ray cluster.
    serve.start()

    # Create two distinct deployments of the same class as
    # two replicas. Associate each deployment with a unique 'name'.
    # This name can be used as to fetch its respective serve handle.
    # See code below for method 1.
    Deployment.options(name="rep-1", num_replicas=2).deploy("/model/rep-1.pkl")
    Deployment.options(name="rep-2", num_replicas=2).deploy("/model/rep-2.pkl")

    # Get the current list of deployments
    print(serve.list_deployments())

    print("ServerHandle API responses: " + "--" * 5)

    # Method 1) Access each deployment using the ServerHandle API
    for _ in range(2):
        for d_name in ["rep-1", "rep-2"]:
            # Get handle to the each deployment and invoke its method.
            # Which replica the request is dispatched to is determined
            # by the Router actor.
            handle = serve.get_deployment(d_name).get_handle()
            print(f"handle name : {d_name}")
            print(f"prediction  : {ray.get(handle.remote(random()))}")
            print("-" * 2)

    print("HTTP responses: " + "--" * 5)

    # Method 2) Access deployment via HTTP Request
    for _ in range(2):
        for d_name in ["rep-1", "rep-2"]:
            # Send HTTP request along with data payload
            url = f"http://127.0.0.1:8000/{d_name}"
            print(f"handle name : {d_name}")
            print(f"prediction  : {requests.get(url, params= {'data': random()}).text}")
# __serve_example_end__
