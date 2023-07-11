import requests
import starlette
from typing import Dict
from ray import serve
from ray.serve.handle import RayServeHandle


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
        self, adder1: RayServeHandle, adder2: RayServeHandle, combiner: RayServeHandle
    ):
        self._adder1, self._adder2, self._combiner = adder1, adder2, combiner

    async def __call__(self, request: starlette.requests.Request) -> Dict[str, float]:
        input_json = await request.json()

        adder1_result = await self._adder1.add.remote(input_json["val"])
        adder2_result = await self._adder2.add.remote(input_json["val"])
        final_result = await self._combiner.average.remote(adder1_result, adder2_result)

        return {"result": await final_result}


# 2. Build the application consisting of the models and ingress.
app = Ingress.bind(Adder.bind(increment=1), Adder.bind(increment=2), Combiner.bind())
serve.run(app)

# 3: Query the application and print the result.
print(requests.post("http://localhost:8000/", json={"val": 100.0}).json())
# {"result": 101.5}
