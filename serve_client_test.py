import ray
from ray import serve

ray.util.connect("localhost:61234")

#serve.start(detached=True)

client = serve.connect()

def f():
    return "Hello!"

#client.create_backend("b", f)
#client.create_endpoint("e", backend="b")

handle = client.get_handle("e")
print(ray.get(handle.remote()))
