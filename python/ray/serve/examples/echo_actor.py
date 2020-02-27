"""
Example actor that adds an increment to a number. This number can
come from either web (parsing Flask request) or python call.

This actor can be called from HTTP as well as from Python.
"""

import time

import requests

import ray
from ray import serve
from ray.serve.utils import pformat_color_json


class MagicCounter:
    def __init__(self, increment):
        self.increment = increment

    def __call__(self, flask_request, base_number=None):
        if serve.context.web:
            base_number = int(flask_request.args.get("base_number", "0"))
        return base_number + self.increment


serve.init(blocking=True)
serve.create_endpoint("magic_counter", "/counter", blocking=True)
serve.create_backend(MagicCounter, "counter:v1", 42)  # increment=42
serve.link("magic_counter", "counter:v1")

print("Sending ten queries via HTTP")
for i in range(10):
    url = "http://127.0.0.1:8000/counter?base_number={}".format(i)
    print("> Pinging {}".format(url))
    resp = requests.get(url).json()
    print(pformat_color_json(resp))

    time.sleep(0.2)

print("Sending ten queries via Python")
handle = serve.get_handle("magic_counter")
for i in range(10):
    print("> Pinging handle.remote(base_number={})".format(i))
    result = ray.get(handle.remote(base_number=i))
    print("< Result {}".format(result))
