"""
Example of error handling mechanism in ray serve

- HTTP server should respond with "internal error"
- ray.get(handle.remote(33)) should raise RayTaskError
"""

import time
from pprint import pprint

import requests

import ray
import ray.experimental.serve as srv


def echo(context):
    raise Exception("Something went wrong...")
    return context


srv.init(blocking=True)

srv.create_endpoint("my_endpoint", "/echo", blocking=True)
srv.create_backend(echo, "echo:v1")
srv.link("my_endpoint", "echo:v1")

for _ in range(2):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

handle = srv.get_handle("my_endpoint")

ray.get(handle.remote(33))
