import requests
from pydantic import BaseModel

import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.dag.input_node import InputNode


ray.init()
serve.start()


class ModelInputData(BaseModel):
    model_input1: int
    model_input2: str


@serve.deployment
class Model:
    def __init__(self, weight):
        self.weight = weight

    def forward(self, input: ModelInputData):
        return input.model_input1 + len(input.model_input2) + self.weight


@serve.deployment
def combine(value_refs):
    return sum(ray.get(value_refs))


with InputNode() as user_input:
    model1 = Model.bind(0)
    model2 = Model.bind(1)
    output1 = model1.forward.bind(user_input)
    output2 = model2.forward.bind(user_input)
    dag = combine.bind([output1, output2])
    serve_dag = DAGDriver.options(route_prefix="/my-dag").bind(
        dag, http_adapter=ModelInputData
    )

dag_handle = serve.run(serve_dag)

print(
    ray.get(
        dag_handle.predict.remote(ModelInputData(model_input1=1, model_input2="test"))
    )
)
print(
    requests.post(
        "http://127.0.0.1:8000/my-dag", json={"model_input1": 1, "model_input2": "test"}
    ).text
)
