"""
Full example of ray.serve module
"""

import ray
import ray.experimental.serve as serve
import requests
import time

# initialize ray serve system.
# blocking=True will wait for HTTP server to be ready to serve request.
serve.init(blocking=True)

# an endpoint is associated with an http URL.
serve.create_endpoint("my_endpoint", "/echo")


# a backend can be a function or class.
def echo_v1(request):
    return request


serve.create_backend(echo_v1, "echo:v1")

# We can link an endpoint to a backend, the means all the traffic
# goes to my_endpoint will now goes to echo:v1 backend.
serve.link("my_endpoint", "echo:v1")

print(requests.get("http://127.0.0.1:8000/echo").json())
# The service will be reachable from http

print(ray.get(serve.get_handle("my_endpoint").remote("hello")))

# as well as within the ray system.


# We can also add a new backend and split the traffic.
def echo_v2(request):
    # magic
    return "something new"


serve.create_backend(echo_v2, "echo:v2")

# The two backend will now split the traffic 50%-50%.
serve.split("my_endpoint", {"echo:v1": 0.5, "echo:v2": 0.5})

# Observe requests are now split between two backends.
for _ in range(10):
    print(requests.get("http://127.0.0.1:8000/echo").json())
    time.sleep(0.5)
