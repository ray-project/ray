import ray
from ray import serve
from ray.experimental.dag.input_node import InputNode


ray.init()
serve.start()


@serve.deployment
class Model:
    def __init__(self, weight):
        self.weight = weight

    def forward(self, input):
        return input + self.weight


# 3 nodes chain in a line
num_nodes = 3
nodes = [Model.bind(w) for w in range(num_nodes)]
outputs = [None] * num_nodes
with InputNode() as dag_input:
    for i in range(num_nodes):
        if i == 0:
            # first node
            outputs[i] = nodes[i].forward.bind(dag_input)
        else:
            outputs[i] = nodes[i].forward.bind(outputs[i - 1])

print(ray.get(outputs[-1].execute(0)))
