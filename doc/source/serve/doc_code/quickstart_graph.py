import requests
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.dag import InputNode
from ray.serve.http_adapters import json_request


# 1. Define the models in our composition graph
@serve.deployment
class Adder:
    def __init__(self, increment: int):
        self.increment = increment

    def predict(self, inp: int):
        return self.increment + inp


@serve.deployment
def combine_average(*input_values) -> float:
    return {"result": sum(input_values) / len(input_values)}


# 2: Define the model composition graph and call it.
with InputNode() as input_node:
    adder_1 = Adder.bind(increment=1)
    adder_2 = Adder.bind(increment=2)
    dag = combine_average.bind(
        adder_1.predict.bind(input_node), adder_2.predict.bind(input_node)
    )

serve.run(DAGDriver.bind(dag, http_adapter=json_request))

# 3: Query the deployment and print the result.
print(requests.post("http://localhost:8000/", json=100).json())
# {"result": 101.5}
