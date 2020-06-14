"""
Example service that prints out http context.
"""

import time

import requests

from ray import serve
from ray.serve.utils import pformat_color_json


def echo(flask_request):
    return "hello " + flask_request.args.get("name", "serve!")


serve.init()

serve.create_backend("echo:v1", echo)
serve.create_endpoint("my_endpoint", backend="echo:v1", route="/echo")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
