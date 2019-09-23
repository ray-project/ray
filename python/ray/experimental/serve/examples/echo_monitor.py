"""
Example service that prints out metric information
"""

import time

import requests

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json


def echo(context):
    time.sleep(0.2)
    return context

def throw(_):
    raise Exception("oh no")


serve.init(blocking=True)

serve.create_endpoint("echo", "/echo", blocking=True)
serve.create_backend(echo, "echo:v1")
serve.create_backend(throw, "echo:v2")
serve.split("echo", {"echo:v1": 0.5, "echo:v2": 0.5})
serve.set_replica("echo:v1", 2)


handle = serve.get_handle("echo")
for _ in range(20):
    handle.remote("")

for _ in range(5):
    print("Getting stats")
    print(pformat_color_json(serve.stat()))
    time.sleep(2)

