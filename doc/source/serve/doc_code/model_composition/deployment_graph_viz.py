# File name: deployment_graph_viz.py

from ray import serve
from ray.serve.deployment_graph import InputNode
from ray.dag.vis_utils import _dag_to_dot


@serve.deployment
class Model:
    def __init__(self, weight: int):
        self.weight = weight

    def forward(self, input: int) -> int:
        return input + self.weight


@serve.deployment
def combine(output_1: int, output_2: int, kwargs_output: int = 0) -> int:
    return output_1 + output_2 + kwargs_output


m1 = Model.bind(1)
m2 = Model.bind(2)

with InputNode() as user_input:
    m1_output = m1.forward.bind(user_input[0])
    m2_output = m2.forward.bind(user_input[1])
    combine_output = combine.bind(m1_output, m2_output, kwargs_output=user_input[2])

# m1_output visualization
graph = _dag_to_dot(m1_output)
to_string = graph.to_string()
print(to_string)

# Full graph visualization
graph = _dag_to_dot(combine_output)
to_string = graph.to_string()
print(to_string)
