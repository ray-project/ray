"""
Example actor that adds message to the end of query_string.
"""

import time

import requests
from werkzeug import urls

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json


class EchoActor:
    def __init__(self, message):
        self.message = message

    def __call__(self, context):
        query_string_dict = urls.url_decode(context["query_string"])
        message = ""
        message += query_string_dict.get("message", "")
        message += " "
        message += self.message
        return message


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo", blocking=True)
serve.create_backend(EchoActor, "echo:v1", "world")
serve.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo?message=hello").json()
    print(pformat_color_json(resp))

    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
