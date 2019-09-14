"""
Example rollback action in ray serve. We first deploy only v1, then set a
 50/50 deployment between v1 and v2, and finally roll back to only v1.
"""
import time

import requests

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json


def echo_v1(_):
    return "v1"


def echo_v2(_):
    return "v2"


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo", blocking=True)
serve.create_backend(echo_v1, "echo:v1")
serve.link("my_endpoint", "echo:v1")

for _ in range(3):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

serve.create_backend(echo_v2, "echo:v2")
serve.split("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})

for _ in range(6):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)

serve.rollback("my_endpoint")
for _ in range(6):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
