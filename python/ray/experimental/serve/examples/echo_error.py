"""
Example of error handling mechanism in ray serve

- HTTP server should respond with "internal error"
- ray.get(handle.remote(33)) should raise RayTaskError
"""

import time
from pprint import pprint

import requests

import ray
from ray.experimental import serve


def echo(context):
    raise Exception("Something went wrong...")
    return context


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo", blocking=True)
serve.create_backend(echo, "echo:v1")
serve.link("my_endpoint", "echo:v1")

for _ in range(2):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

handle = serve.get_handle("my_endpoint")

ray.get(handle.remote(33))
