# __graph_start__
# File name: branching_input.py

import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request
from ray.serve.deployment_graph import InputNode


@serve.deployment
class Model:
    def __init__(self, weight: int):
        self.weight = weight

    def forward(self, input: int) -> int:
        return input + self.weight


@serve.deployment
def combine(value_refs):
    return sum(ray.get(value_refs))


model1 = Model.bind(0)
model2 = Model.bind(1)

with InputNode() as user_input:
    output1 = model1.forward.bind(user_input)
    output2 = model2.forward.bind(user_input)
    combine_output = combine.bind([output1, output2])

graph = DAGDriver.bind(combine_output, http_adapter=json_request)

handle = serve.run(graph)

sum = ray.get(handle.predict.remote(1))
print(sum)
# __graph_end__

assert sum == 3
