import ray
from ray import serve
import requests

ray.init()
client = serve.start()

def say_hello(request):
    return "hello " + request.query_params["name"]

client.create_backend("my_backend", say_hello)
client.create_endpoint("my_endpoint", backend="my_backend", route="/hello")

print(requests.get("http://127.0.0.1:8000/hello?name=serve").text)
# > hello serve!
print(ray.get(client.get_handle("my_endpoint").remote(name="serve")))
# > hello serve!