"""
Ray serve pipeline example
"""
import ray
import ray.serve as serve
import time

# Initialize ray serve instance.
serve.start()


# A backend can be a function or class.
# It can be made to be invoked via HTTP as well as python.
def echo_v1(_, response="hello from python!"):
    return f"echo_v1({response})"


serve.create_backend("echo_v1", echo_v1)
serve.create_endpoint("echo_v1", backend="echo_v1", route="/echo_v1")


def echo_v2(_, relay=""):
    return f"echo_v2({relay})"


serve.create_backend("echo_v2", echo_v2)
serve.create_endpoint("echo_v2", backend="echo_v2", route="/echo_v2")


def echo_v3(_, relay=""):
    return f"echo_v3({relay})"


serve.create_backend("echo_v3", echo_v3)
serve.create_endpoint("echo_v3", backend="echo_v3", route="/echo_v3")


def echo_v4(_, relay1="", relay2=""):
    return f"echo_v4({relay1} , {relay2})"


serve.create_backend("echo_v4", echo_v4)
serve.create_endpoint("echo_v4", backend="echo_v4", route="/echo_v4")
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
