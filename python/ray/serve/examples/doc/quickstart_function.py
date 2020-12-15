import ray
from ray import serve
import requests

ray.init(num_cpus=8)
client = serve.start()


def echo(starlette_request):
    return "hello " + starlette_request.query_params.get("name", "serve!")


client.create_backend("hello", echo)
client.create_endpoint("hello", backend="hello", route="/hello")

print(requests.get("http://127.0.0.1:8000/hello").text)
# > hello serve!
