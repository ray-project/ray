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
def combine(value_refs, combine_type):
    values = [ray.get(value_ref) for value_ref in value_refs]
    if combine_type == "sum":
        return sum(values)
    else:
        return max(values)


with InputNode() as user_input:
    model1 = Model.bind(0)
    model2 = Model.bind(1)
    output1 = model1.forward.bind(user_input[0])
    output2 = model2.forward.bind(user_input[0])
    dag = combine.bind([output1, output2], user_input[1])

print(ray.get(dag.execute(1, "max")))
print(ray.get(dag.execute(1, "sum")))
