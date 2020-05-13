"""
Example of traffic splitting. We will first use echo:v1. Then v1 and v2
will split the incoming traffic evenly.
"""
import time

import requests

from ray import serve
from ray.serve.utils import pformat_color_json


def echo_v1(_):
    return "v1"


def echo_v2(_):
    return "v2"


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo")
serve.create_backend("echo:v1", echo_v1)
serve.set_traffic("my_endpoint", {"echo:v1": 1.0})

for _ in range(3):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

serve.create_backend("echo:v2", echo_v2)
serve.set_traffic("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})
while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
