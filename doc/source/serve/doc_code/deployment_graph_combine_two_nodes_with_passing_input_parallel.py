import ray
from ray import serve
from ray.serve.deployment_graph import InputNode


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


with InputNode() as user_input:
    model1 = Model.bind(0)
    model2 = Model.bind(1)
    output1 = model1.forward.bind(user_input)
    output2 = model2.forward.bind(user_input)
    dag = combine.bind([output1, output2])

print(ray.get(dag.execute(1)))
