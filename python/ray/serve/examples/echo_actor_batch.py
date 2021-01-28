"""
Example actor that adds an increment to a number. This number can
come from either web (parsing Starlette request) or python call.
The queries incoming to this actor are batched.
This actor can be called from HTTP as well as from Python.
"""

import json
import time

from pygments import formatters, highlight, lexers

import requests

import ray
from ray import serve


def pformat_color_json(d):
    """Use pygments to pretty format and colorize dictionary"""
    formatted_json = json.dumps(d, sort_keys=True, indent=4)

    colorful_json = highlight(formatted_json, lexers.JsonLexer(),
                              formatters.TerminalFormatter())

    return colorful_json


class MagicCounter:
    def __init__(self, increment):
        self.increment = increment

    @serve.accept_batch
    def __call__(self, starlette_request_list, base_number=None):
        # batch_size = serve.context.batch_size
        if serve.context.web:
            result = []
            for starlette_request in starlette_request_list:
                base_number = int(
                    starlette_request.query_params.get("base_number", "0"))
                result.append(base_number)
            return list(map(lambda x: x + self.increment, result))
        else:
            result = []
            for b in base_number:
                ans = b + self.increment
                result.append(ans)
            return result


client = serve.start()
client.create_backend(
    "counter:v1", MagicCounter, 42,
    config={"max_batch_size": 5})  # increment=42
client.create_endpoint("magic_counter", backend="counter:v1", route="/counter")

print("Sending ten queries via HTTP")
for i in range(10):
    url = "http://127.0.0.1:8000/counter?base_number={}".format(i)
    print("> Pinging {}".format(url))
    resp = requests.get(url).json()
    print(pformat_color_json(resp))

    time.sleep(0.2)

print("Sending ten queries via Python")
handle = client.get_handle("magic_counter")
for i in range(10):
    print("> Pinging handle.remote(base_number={})".format(i))
    result = ray.get(handle.remote(base_number=i))
    print("< Result {}".format(result))
