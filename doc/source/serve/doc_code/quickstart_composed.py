import requests
import starlette
from typing import Dict
from ray import serve
from ray.serve.handle import DeploymentHandle


# 1. Define the models in our composition graph and an ingress that calls them.
@serve.deployment
class Adder:
    def __init__(self, increment: int):
        self.increment = increment

    def add(self, inp: int):
        return self.increment + inp


@serve.deployment
class Combiner:
    def average(self, *inputs) -> float:
        return sum(inputs) / len(inputs)


@serve.deployment
class Ingress:
    def __init__(
        self,
        adder1: DeploymentHandle,
        adder2: DeploymentHandle,
        combiner: DeploymentHandle,
    ):
        self._adder1 = adder1
        self._adder2 = adder2
        self._combiner = combiner

    async def __call__(self, request: starlette.requests.Request) -> Dict[str, float]:
        input_json = await request.json()
        final_result = await self._combiner.average.remote(
            self._adder1.add.remote(input_json["val"]),
            self._adder2.add.remote(input_json["val"]),
        )
        return {"result": final_result}


# 2. Build the application consisting of the models and ingress.
app = Ingress.bind(Adder.bind(increment=1), Adder.bind(increment=2), Combiner.bind())
serve.run(app)

# 3: Query the application and print the result.
print(requests.post("http://localhost:8000/", json={"val": 100.0}).json())
# {"result": 101.5}
