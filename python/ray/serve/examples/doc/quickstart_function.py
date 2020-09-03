import ray
from ray import serve
import requests

ray.init(num_cpus=8)
serve.init()


def echo(flask_request):
    return "hello " + flask_request.args.get("name", "serve!")


serve.create_backend("hello", echo)
serve.create_endpoint("hello", backend="hello", route="/hello")

requests.get("http://127.0.0.1:8000/hello").text
# > "hello serve!"
