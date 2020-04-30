"""
Full example of ray.serve module
"""

import time

import requests

import ray
import ray.serve as serve
from ray.serve.utils import pformat_color_json

# initialize ray serve system.
# blocking=True will wait for HTTP server to be ready to serve request.
serve.init(blocking=True)

# an endpoint is associated with an http URL.
serve.create_endpoint("my_endpoint", "/echo")


# a backend can be a function or class.
# it can be made to be invoked from web as well as python.
def echo_v1(flask_request, response="hello from python!"):
    if serve.context.web:
        response = flask_request.url
    return response


serve.create_backend("echo:v1", echo_v1)

# We can link an endpoint to a backend, the means all the traffic
# goes to my_endpoint will now goes to echo:v1 backend.
serve.set_traffic("my_endpoint", {"echo:v1": 1.0})

print(requests.get("http://127.0.0.1:8000/echo", timeout=0.5).text)
# The service will be reachable from http

print(ray.get(serve.get_handle("my_endpoint").remote(response="hello")))

# as well as within the ray system.


# We can also add a new backend and split the traffic.
def echo_v2(flask_request):
    # magic, only from web.
    return "something new"


serve.create_backend("echo:v2", echo_v2)

# The two backend will now split the traffic 50%-50%.
serve.set_traffic("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})

# Observe requests are now split between two backends.
for _ in range(10):
    print(requests.get("http://127.0.0.1:8000/echo").text)
    time.sleep(0.5)

# You can also change number of replicas for each backend independently.
serve.update_backend_config("echo:v1", {"num_replicas": 2})
serve.update_backend_config("echo:v2", {"num_replicas": 2})

# As well as retrieving relevant system metrics
print(pformat_color_json(serve.stat()))
