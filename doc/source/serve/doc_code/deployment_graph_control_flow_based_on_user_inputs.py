import ray
from ray import serve
from ray.dag.input_node import InputNode


ray.init()


@serve.deployment
class Model:
    def __init__(self, weight):
        self.weight = weight

    def forward(self, input):
        return input + self.weight


@serve.deployment
def combine(value1, value2, combine_type):
    if combine_type == "sum":
        return sum([value1, value2])
    else:
        return max([value1, value2])


with InputNode() as user_input:
    model1 = Model.bind(0)
    model2 = Model.bind(1)
    output1 = model1.forward.bind(user_input[0])
    output2 = model2.forward.bind(user_input[0])
    dag = combine.bind(output1, output2, user_input[1])


print(ray.get(dag.execute(1, "max")))
print(ray.get(dag.execute(1, "sum")))
