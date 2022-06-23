# __fruit_example_begin__
# File name: fruit.py

import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.deployment_graph import InputNode
from ray.serve.http_adapters import json_request

# These imports are used only for type hints:
from typing import Dict, List
from starlette.requests import Request
from ray.serve.deployment_graph import ClassNode


@serve.deployment(num_replicas=2)
class FruitMarket:

    def __init__(
        self,
        mango_stand: ClassNode,
        orange_stand: ClassNode,
        pear_stand: ClassNode,
    ):
        self.directory = {
            "MANGO": mango_stand,
            "ORANGE": orange_stand,
            "PEAR": pear_stand,
        }
    
    def check_price(self, fruit: str, amount: float) -> float:
        if fruit not in self.directory:
            return -1
        else:
            fruit_stand = self.directory[fruit]
            return ray.get(fruit_stand.check_price.remote(amount))


@serve.deployment(user_config={"price": 3})
class MangoStand:

    DEFAULT_PRICE = 1

    def __init__(self):
        # This default price is overwritten by the one specified in the
        # user_config through the reconfigure() method.
        self.price = self.DEFAULT_PRICE
    
    def reconfigure(self, config: Dict):
        self.price = config.get("price", self.DEFAULT_PRICE)
    
    def check_price(self, amount: float) -> float:
        return self.price * amount


@serve.deployment(user_config={"price": 2})
class OrangeStand:

    DEFAULT_PRICE = 0.5

    def __init__(self):
        # This default price is overwritten by the one specified in the
        # user_config through the reconfigure() method.
        self.price = self.DEFAULT_PRICE
    
    def reconfigure(self, config: Dict):
        self.price = config.get("price", self.DEFAULT_PRICE)
    
    def check_price(self, amount: float) -> float:
        return self.price * amount


@serve.deployment(user_config={"price": 4})
class PearStand:

    DEFAULT_PRICE = 0.75

    def __init__(self):
        # This default price is overwritten by the one specified in the
        # user_config through the reconfigure() method.
        self.price = self.DEFAULT_PRICE
    
    def reconfigure(self, config: Dict):
        self.price = config.get("price", self.DEFAULT_PRICE)
    
    def check_price(self, amount: float) -> float:
        return self.price * amount


async def json_resolver(request: Request) -> List:
    return await request.json()


with InputNode() as query:
    fruit, amount = query[0], query[1]

    mango_stand = MangoStand.bind()
    orange_stand = OrangeStand.bind()
    pear_stand = PearStand.bind()

    fruit_market = FruitMarket.bind(mango_stand, orange_stand, pear_stand)

    net_price = fruit_market.check_price.bind(fruit, amount)

deployment_graph = DAGDriver.bind(net_price, http_adapter=json_request)
# __fruit_example_end__

serve.start()
serve.run(deployment_graph)

import requests
assert requests.post("http://localhost:8000/", json=["MANGO", 1]).json() == 3
assert requests.post("http://localhost:8000/", json=["ORANGE", 1]).json() == 2
assert requests.post("http://localhost:8000/", json=["PEAR", 1]).json() == 4
assert requests.post("http://localhost:8000/", json=["TOMATO", 1]).json() == -1

MangoStand.options(user_config={"price": 0}).deploy()
assert requests.post("http://localhost:8000/", json=["MANGO", 1]).json() == 0

OrangeStand.options(user_config={"price": 0}).deploy()
assert requests.post("http://localhost:8000/", json=["ORANGE", 1]).json() == 0

PearStand.options(user_config={"price": 0}).deploy()
assert requests.post("http://localhost:8000/", json=["PEAR", 1]).json() == 0
