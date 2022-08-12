# __graph_start__
# File name: conditional.py

import ray
from ray import serve
from ray.dag.input_node import InputNode


@serve.deployment
class Model:
    def __init__(self, weight: int):
        self.weight = weight

    def forward(self, input: int) -> int:
        return input + self.weight


@serve.deployment
def combine(value1: int, value2: int, operation: str) -> int:
    if operation == "sum":
        return sum([value1, value2])
    else:
        return max([value1, value2])


model1 = Model.bind(0)
model2 = Model.bind(1)

with InputNode() as user_input:
    input_number, input_operation = user_input[0], user_input[1]
    output1 = model1.forward.bind(input_number)
    output2 = model2.forward.bind(input_number)
    combine_output = combine.bind(output1, output2, input_operation)

max_output = ray.get(combine_output.execute(1, "max"))
print(max_output)

sum_output = ray.get(combine_output.execute(1, "sum"))
print(sum_output)
# __graph_end__

assert max_output == 2
assert sum_output == 3
