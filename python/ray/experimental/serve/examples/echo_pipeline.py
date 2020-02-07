"""
Ray serve pipeline example
"""
import ray
import ray.experimental.serve as serve
import time

# initialize ray serve system.
# blocking=True will wait for HTTP server to be ready to serve request.
serve.init(blocking=True)


# a backend can be a function or class.
# it can be made to be invoked from web as well as python.
def echo_v1(_, response="hello from python!"):
    return f"echo_v1({response})"


def echo_v2(_, relay=""):
    return f"echo_v2({relay})"


def echo_v3(_, relay=""):
    return f"echo_v3({relay})"


def echo_v4(_, relay1="", relay2=""):
    return f"echo_v4({relay1} , {relay2})"


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

# an endpoint is associated with an http URL.
serve.create_endpoint("my_endpoint1", "/echo1")
serve.create_endpoint("my_endpoint2", "/echo2")
serve.create_endpoint("my_endpoint3", "/echo3")
serve.create_endpoint("my_endpoint4", "/echo4")

# create backends
serve.create_backend(echo_v1, "echo:v1")
serve.create_backend(echo_v2, "echo:v2")
serve.create_backend(echo_v3, "echo:v3")
serve.create_backend(echo_v4, "echo:v4")

# link service to backends
serve.link("my_endpoint1", "echo:v1")
serve.link("my_endpoint2", "echo:v2")
serve.link("my_endpoint3", "echo:v3")
serve.link("my_endpoint4", "echo:v4")

# get the handle of the endpoints
handle1 = serve.get_handle("my_endpoint1")
handle2 = serve.get_handle("my_endpoint2")
handle3 = serve.get_handle("my_endpoint3")
handle4 = serve.get_handle("my_endpoint4")

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
