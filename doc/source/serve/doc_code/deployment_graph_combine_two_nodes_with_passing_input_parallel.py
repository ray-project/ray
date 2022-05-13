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


@serve.deployment
def combine(value_refs):
    return sum(ray.get(value_refs))


number_nodes = 2
nodes = [Model.bind(weight) for weight in range(0, number_nodes)]
outputs = []
with InputNode() as user_input:
    for i in range(0, number_nodes):
        outputs.append(nodes[i].forward.bind(user_input))
    dag = combine.bind(outputs)

print(ray.get(dag.execute(1)))
