"""
Example service that prints out http context.
"""

import time
from pprint import pprint

import requests

from ray.experimental import serve


def echo(context):
    return context


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo", blocking=True)
serve.create_backend(echo, "echo:v1")
serve.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
