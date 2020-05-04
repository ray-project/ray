"""
Ray serve pipeline example
"""
import ray
import ray.serve as serve
import time

# initialize ray serve system.
# blocking=True will wait for HTTP server to be ready to serve request.
serve.init(blocking=True)


# a backend can be a function or class.
# it can be made to be invoked from web as well as python.
def echo_v1(_, response="hello from python!"):
    return f"echo_v1({response})"


serve.create_endpoint("echo_v1", "/echo_v1")
serve.create_backend("echo_v1", echo_v1)
serve.set_traffic("echo_v1", {"echo_v1": 1.0})


def echo_v2(_, relay=""):
    return f"echo_v2({relay})"


serve.create_endpoint("echo_v2", "/echo_v2")
serve.create_backend("echo_v2", echo_v2)
serve.set_traffic("echo_v2", {"echo_v2": 1.0})


def echo_v3(_, relay=""):
    return f"echo_v3({relay})"


serve.create_endpoint("echo_v3", "/echo_v3")
serve.create_backend("echo_v3", echo_v3)
serve.set_traffic("echo_v3", {"echo_v3": 1.0})


def echo_v4(_, relay1="", relay2=""):
    return f"echo_v4({relay1} , {relay2})"


serve.create_endpoint("echo_v4", "/echo_v4")
serve.create_backend("echo_v4", echo_v4)
serve.set_traffic("echo_v4", {"echo_v4": 1.0})
"""
The pipeline created is as follows -
            "my_endpoint1"
                  /\
                 /  \
                /    \
               /      \
              /        \
             /          \
  "my_endpoint2"     "my_endpoint3"
            \            /
             \          /
              \        /
               \      /
                \    /
                 \  /
                  \/
            "my_endpoint4"
"""

# get the handle of the endpoints
handle1 = serve.get_handle("echo_v1")
handle2 = serve.get_handle("echo_v2")
handle3 = serve.get_handle("echo_v3")
handle4 = serve.get_handle("echo_v4")

start = time.time()
print("Start firing to the pipeline: {} s".format(time.time()))
handle1_oid = handle1.remote(response="hello")
handle4_oid = handle4.remote(
    relay1=handle2.remote(relay=handle1_oid),
    relay2=handle3.remote(relay=handle1_oid))
print("Firing ended now waiting for the result,"
      "time taken: {} s".format(time.time() - start))
result = ray.get(handle4_oid)
print("Result: {}, time taken: {} s".format(result, time.time() - start))
