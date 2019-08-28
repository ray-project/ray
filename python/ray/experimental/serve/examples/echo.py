"""
Example service that prints out http context
"""

import time
from pprint import pprint

import requests

import ray.experimental.serve as srv


def echo(context):
    return context


srv.init(blocking=True)

srv.create_endpoint("my_endpoint", "/echo", blocking=True)
srv.create_backend(echo, "echo:v1")
srv.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
