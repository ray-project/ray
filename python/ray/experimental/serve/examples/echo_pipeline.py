"""
Ray serve pipeline example
"""
import ray
import ray.experimental.serve as serve

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
# get ObjectID and wall clock time
# this wall clock time will be passed to further remote calls
first_object_id, wall_clock_slo = handle1.remote(
    response="hello", return_wall_clock_time=True)

# create ObjectIDs for getting the result
second_object_id = ray.ObjectID.from_random()
third_object_id = ray.ObjectID.from_random()

# pass the wall clock time and ObjectIDS. This remote will be completely
# asynchronous! All the remote calls below are completely asynchronous
temp1 = handle2.remote(
    relay=first_object_id,
    return_object_ids=[second_object_id],
    slo_ms=wall_clock_slo,
    is_wall_clock_time=True)

# check for this call to be asynchronous
assert temp1 is None
handle3.remote(
    relay=first_object_id,
    return_object_ids=[third_object_id],
    slo_ms=wall_clock_slo,
    is_wall_clock_time=True)
fourth_object_id = ray.ObjectID.from_random()
temp2 = handle4.remote(
    relay1=second_object_id,
    relay2=third_object_id,
    return_object_ids=[fourth_object_id],
    slo_ms=wall_clock_slo,
    is_wall_clock_time=True)
assert temp2 is None
print(ray.get(fourth_object_id))
