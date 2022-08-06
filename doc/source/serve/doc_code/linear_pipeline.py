# __graph_start__
# File name: linear_pipeline.py

import ray
from ray import serve
from ray.serve.deployment_graph import InputNode


@serve.deployment
class Model:
    def __init__(self, weight: float):
        self.weight = weight

    def forward(self, input: float) -> float:
        return input + self.weight


nodes = [Model.bind(0), Model.bind(1), Model.bind(2)]
outputs = [None, None, None]

with InputNode() as graph_input:
    outputs[0] = nodes[0].forward.bind(graph_input)

    for i in range(1, len(nodes)):
        outputs[i] = nodes[i].forward.bind(outputs[i - 1])

last_output_node = outputs[-1]

sum = ray.get(last_output_node.execute(0))
print(sum)
# __graph_end__

assert sum == 3
