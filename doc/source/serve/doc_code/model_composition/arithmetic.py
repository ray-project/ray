# flake8: noqa

# __graph_start__
# File name: arithmetic.py
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.deployment_graph import InputNode

from starlette.requests import Request


@serve.deployment
class AddCls:
    def __init__(self, addend: float):
        self.addend = addend

    def add(self, number: float) -> float:
        return number + self.addend

    async def unpack_request(self, http_request: Request) -> float:
        return await http_request.json()


@serve.deployment
def subtract_one_fn(number: float) -> float:
    return number - 1


@serve.deployment
async def unpack_request(http_request: Request) -> float:
    return await http_request.json()


add_2 = AddCls.bind(2)
add_3 = AddCls.bind(3)

with InputNode() as http_request:
    request_number = unpack_request.bind(http_request)
    add_2_output = add_2.add.bind(request_number)
    subtract_1_output = subtract_one_fn.bind(add_2_output)
    add_3_output = add_3.add.bind(subtract_1_output)

graph = DAGDriver.bind(add_3_output)
# __graph_end__

serve.run(graph)

# __graph_client_start__
# File name: arithmetic_client.py
import requests

response = requests.post("http://localhost:8000/", json=5)
output = response.json()
print(output)
# __graph_client_end__

assert output == 9

# __adapter_graph_start__
# This import can go to the top of the file.
from ray.serve.http_adapters import json_request

add_2 = AddCls.bind(2)
add_3 = AddCls.bind(3)

with InputNode() as request_number:
    add_2_output = add_2.add.bind(request_number)
    subtract_1_output = subtract_one_fn.bind(add_2_output)
    add_3_output = add_3.add.bind(subtract_1_output)

graph = DAGDriver.bind(add_3_output, http_adapter=json_request)
# __adapter_graph_end__

serve.run(graph)
assert requests.post("http://localhost:8000/", json=5).json() == 9

# __test_graph_start__
# These imports can go to the top of the file.
import ray
from ray.serve.http_adapters import json_request

add_2 = AddCls.bind(2)
add_3 = AddCls.bind(3)

with InputNode() as request_number:
    add_2_output = add_2.add.bind(request_number)
    subtract_1_output = subtract_one_fn.bind(add_2_output)
    add_3_output = add_3.add.bind(subtract_1_output)

graph = DAGDriver.bind(add_3_output, http_adapter=json_request)

handle = serve.run(graph)

ref = handle.predict.remote(5)
result = ray.get(ref)
print(result)
# __test_graph_end__

assert result == 9
