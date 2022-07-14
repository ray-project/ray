import ray
from ray import serve
from ray.serve.deployment_graph import InputNode
import time
from ray.dag.gradio_utils import GraphVisualizer

ray.init()


@serve.deployment
class Model2:
    def __init__(self, weight):
        self.weight = weight

    def forward(self, input1, input2):
        time.sleep(1)
        return (input1 + input2) * self.weight


@serve.deployment
class Model:
    def __init__(self, weight):
        self.weight = weight

    def forward(self, input):
        time.sleep(1)
        return input * self.weight


@serve.deployment
def combine(output_1, output_2, kwargs_output=0):
    time.sleep(1)
    return output_1 + output_2 + kwargs_output


with InputNode() as user_input:
    l = Model2.bind(2)
    m1 = Model.bind(2)
    m2 = Model.bind(2)
    l_output = l.forward.bind(user_input[0], user_input[1])
    m1_output = m1.forward.bind(l_output)
    m2_output = m2.forward.bind(user_input[2])
    dag = combine.bind(m1_output, m2_output, kwargs_output=user_input[3])

visualizer = GraphVisualizer()
visualizer.visualize_with_gradio(dag)
