"""
This example demostrates replica scaling ability. It implements a service
that returns its own replica ids. The example will show case the following
states:
1. Just one replica
2. Scale 1->2
3. Blacklist the first replica. You should see only the second replica is
   serving the query
4. Scale 2->8. More replicas joined.
5. 8->0. You should see the final request will be blocking.
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
        message += " my id: {}".format(getattr(self, "_ray_serve_replica_id", "unknown"))
        return message


serve.init(blocking=True)

serve.create_endpoint("my_endpoint", "/echo", blocking=True)
serve.create_backend(EchoActor, "echo:v1", "world")
serve.link("my_endpoint", "echo:v1")
serve.set_replica("echo:v1", 2)

for _ in range(10):
    resp = requests.get("http://127.0.0.1:8000/echo?message=hello").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 1 seconds...")
    time.sleep(1)

print('-'*40)
print("Blacklisting replica 0")
serve.blacklist("echo:v1", 0)
print('-'*40)

for _ in range(10):
    resp = requests.get("http://127.0.0.1:8000/echo?message=hello").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 1 seconds...")
    time.sleep(1)

print('-'*40)
print("Starting 6 more replicas")
serve.set_replica("echo:v1", 8)
print('-'*40)

for _ in range(20):
    resp = requests.get("http://127.0.0.1:8000/echo?message=hello").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 1 seconds...")
    time.sleep(1)

print('-'*40)
print("Scaling down to 0")
serve.set_replica("echo:v1", 0)
print('-'*40)

while True:
    resp = requests.get("http://127.0.0.1:8000/echo?message=hello").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
