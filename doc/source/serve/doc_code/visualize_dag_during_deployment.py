import ray
from ray import serve
from ray.serve.deployment_graph import InputNode

ray.init()


@serve.deployment
class Model:
    def __init__(self, weight):
        self.weight = weight

    def forward(self, input):
        return input + self.weight


@serve.deployment
def combine(output_1, output_2, kwargs_output=0):
    return output_1 + output_2 + kwargs_output


with InputNode() as user_input:
    m1 = Model.bind(1)
    m2 = Model.bind(2)
    m1_output = m1.forward.bind(user_input[0])
    m2_output = m2.forward.bind(user_input[1])
    dag = combine.bind(m1_output, m2_output, kwargs_output=user_input[2])

# Partial DAG visualization
graph = ray.experimental.dag.vis_utils.dag_to_dot(m1_output)
to_string = graph.to_string()
print(to_string)

# Entire DAG visualization
graph = ray.experimental.dag.vis_utils.dag_to_dot(dag)
to_string = graph.to_string()
print(to_string)
