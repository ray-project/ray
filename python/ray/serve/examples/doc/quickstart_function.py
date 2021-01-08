import ray
from ray import serve
import requests

ray.init(num_cpus=4)
client = serve.start()


def say_hello(request):
    return "hello " + request.query_params["name"] + "!"


# Form a backend from our function and connect it to an endpoint.
client.create_backend("my_backend", say_hello)
client.create_endpoint("my_endpoint", backend="my_backend", route="/hello")

# Query our endpoint in two different ways: from HTTP and from Python.
print(requests.get("http://127.0.0.1:8000/hello?name=serve").text)
# > hello serve!
print(ray.get(client.get_handle("my_endpoint").remote(name="serve")))
# > hello serve!
