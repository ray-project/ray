"""
Example service that prints out http context.
"""

import time

import requests

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json


def echo(context):
    return context


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo", blocking=True)
serve.create_backend(echo, "echo:v1")
serve.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
