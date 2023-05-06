import requests

# __serve_example_begin__
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.deployment_graph import InputNode
from ray.serve.http_adapters import json_request


@serve.deployment
def greet(name: str):
    return f"Good morning {name}!"


with InputNode() as name:
    greeter = greet.bind(name)

app = DAGDriver.options(route_prefix="/greet").bind(greeter, http_adapter=json_request)
# __serve_example_end__

# Test
if __name__ == "__main__":
    serve.run(app)
    resp = requests.post("http://localhost:8000/greet", json="Bob").json()
    assert resp == "Good morning Bob!"
