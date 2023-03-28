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
