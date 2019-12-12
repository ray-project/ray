"""
SLO [reverse] example of ray.serve module
"""

import time

import requests

import ray
import ray.experimental.serve as serve

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


serve.create_backend(echo_v1, "echo:v1")
serve.link("my_endpoint", "echo:v1")

# wait for routing table to get populated
time.sleep(2)

# slo (10 milliseconds deadline) can be specified via http
slo_ms = 10.0
print("> [HTTP] Pinging http://127.0.0.1:8000/echo?slo_ms={}".format(slo_ms))
print(
    requests.get("http://127.0.0.1:8000/echo?slo_ms={}".format(slo_ms)).json())

# get the handle of the endpoint
handle = serve.get_handle("my_endpoint")

future_list = []

# fire 10 requests with slo's in the (almost) reverse order of the order in
# which remote procedure call is done
for r in range(10):
    slo_ms = 1000 - 100 * r
    response = "hello from request: {} slo: {}".format(r, slo_ms)
    print("> [REMOTE] Pinging handle.remote(response='{}',slo_ms={})".format(
        response, slo_ms))
    # slo can be specified via remote function
    f = handle.remote(response=response, slo_ms=slo_ms)
    future_list.append(f)

# get results of queries as they complete
# should be completed (almost) according to the order of their slo time
left_futures = future_list
while left_futures:
    completed_futures, remaining_futures = ray.wait(left_futures, timeout=0.05)
    if len(completed_futures) > 0:
        result = ray.get(completed_futures[0])
        print(result)
    left_futures = remaining_futures
