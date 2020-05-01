"""
Example of error handling mechanism in ray serve.

We are going to define a buggy function that raise some exception:
>>> def echo(_):
        raise Exception("oh no")

The expected behavior is:
- HTTP server should respond with "internal error" in the response JSON
- ray.get(handle.remote()) should raise RayTaskError with traceback.

This shows that error is hidden from HTTP side but always visible when calling
from Python.
"""

import time

import requests

import ray
from ray import serve
from ray.serve.utils import pformat_color_json


def echo(_):
    raise Exception("Something went wrong...")


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo")
serve.create_backend("echo:v1", echo)
serve.set_traffic("my_endpoint", {"echo:v1": 1.0})

for _ in range(2):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

handle = serve.get_handle("my_endpoint")
print("Invoke from python will raise exception with traceback:")
ray.get(handle.remote())
