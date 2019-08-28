"""
Example of traffic splitting. We will first use echo:v1. Then v1 and v2
will split the incoming traffic evenly.
"""
import time
from pprint import pprint

import requests

import ray.experimental.serve as srv


def echo_v1(context):
    return "v1"


def echo_v2(context):
    return "v2"


srv.init(blocking=True)

srv.create_endpoint("my_endpoint", "/echo", blocking=True)
srv.create_backend(echo_v1, "echo:v1")
srv.link("my_endpoint", "echo:v1")

for _ in range(3):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

srv.create_backend(echo_v2, "echo:v2")
srv.split("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})
while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
