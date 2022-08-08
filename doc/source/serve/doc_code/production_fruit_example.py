# __fruit_example_begin__
# File name: fruit.py

import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.deployment_graph import InputNode
from ray.serve.handle import RayServeDeploymentHandle
from ray.serve.http_adapters import json_request

# These imports are used only for type hints:
from typing import Dict, List
from starlette.requests import Request


@serve.deployment(num_replicas=2)
class FruitMarket:
    def __init__(
        self,
        mango_stand: RayServeDeploymentHandle,
        orange_stand: RayServeDeploymentHandle,
        pear_stand: RayServeDeploymentHandle,
    ):
        self.directory = {
            "MANGO": mango_stand,
            "ORANGE": orange_stand,
            "PEAR": pear_stand,
        }

    async def check_price(self, fruit: str, amount: float) -> float:
        if fruit not in self.directory:
            return -1
        else:
            fruit_stand = self.directory[fruit]
            ref: ray.ObjectRef = await fruit_stand.check_price.remote(amount)
            result = await ref
            return result


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


# Test example's behavior
import requests  # noqa: E402
from ray.serve.schema import ServeApplicationSchema  # noqa: E402
from ray.serve.api import build  # noqa: E402
from ray._private.test_utils import wait_for_condition  # noqa: E402


def check_fruit_deployment_graph():
    """Checks the fruit deployment graph from this example."""

    assert requests.post("http://localhost:8000/", json=["MANGO", 1]).json() == 3
    assert requests.post("http://localhost:8000/", json=["ORANGE", 1]).json() == 2
    assert requests.post("http://localhost:8000/", json=["PEAR", 1]).json() == 4
    assert requests.post("http://localhost:8000/", json=["TOMATO", 1]).json() == -1


def check_fruit_deployment_graph_updates():
    """Checks the graph after updating all prices to 0."""

    assert requests.post("http://localhost:8000/", json=["MANGO", 1]).json() == 0
    assert requests.post("http://localhost:8000/", json=["ORANGE", 1]).json() == 0
    assert requests.post("http://localhost:8000/", json=["PEAR", 1]).json() == 0


# Test behavior from this documentation example
serve.start(detached=True)
app = build(deployment_graph)
for deployment in app.deployments.values():
    deployment.set_options(ray_actor_options={"num_cpus": 0.1})
serve.run(app)
check_fruit_deployment_graph()
MangoStand.options(name="MangoStand", user_config={"price": 0}).deploy()
OrangeStand.options(user_config={"price": 0}).deploy()
PearStand.options(user_config={"price": 0}).deploy()
check_fruit_deployment_graph_updates()
print("Example ran successfully from the file.")
serve.shutdown()

# Check that deployments have been torn down
try:
    requests.post("http://localhost:8000/", json=["MANGO", 1]).json()
    raise ValueError("Deployments should have been torn down!")
except requests.exceptions.ConnectionError:
    pass
print("Deployments have been torn down.")

# Check for regression in remote repository
client = serve.start(detached=True)
config1 = {
    "import_path": "fruit.deployment_graph",
    "runtime_env": {
        "working_dir": (
            "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
        )
    },
    "deployments": [
        {"name": "FruitMarket", "ray_actor_options": {"num_cpus": 0.1}},
        {"name": "MangoStand", "ray_actor_options": {"num_cpus": 0.1}},
        {"name": "OrangeStand", "ray_actor_options": {"num_cpus": 0.1}},
        {"name": "PearStand", "ray_actor_options": {"num_cpus": 0.1}},
        {"name": "DAGDriver", "ray_actor_options": {"num_cpus": 0.1}},
    ],
}
client.deploy_app(ServeApplicationSchema.parse_obj(config1))
wait_for_condition(
    lambda: requests.post("http://localhost:8000/", json=["MANGO", 1]).json() == 3,
    timeout=15,
)
check_fruit_deployment_graph()
config2 = {
    "import_path": "fruit.deployment_graph",
    "runtime_env": {
        "working_dir": (
            "https://github.com/ray-project/serve_config_examples/archive/HEAD.zip"
        )
    },
    "deployments": [
        {"name": "FruitMarket", "ray_actor_options": {"num_cpus": 0.1}},
        {
            "name": "MangoStand",
            "user_config": {"price": 0},
            "ray_actor_options": {"num_cpus": 0.1},
        },
        {
            "name": "OrangeStand",
            "user_config": {"price": 0},
            "ray_actor_options": {"num_cpus": 0.1},
        },
        {
            "name": "PearStand",
            "user_config": {"price": 0},
            "ray_actor_options": {"num_cpus": 0.1},
        },
        {"name": "DAGDriver", "ray_actor_options": {"num_cpus": 0.1}},
    ],
}
client.deploy_app(ServeApplicationSchema.parse_obj(config2))
wait_for_condition(
    lambda: requests.post("http://localhost:8000/", json=["MANGO", 1]).json() == 0,
    timeout=15,
)
check_fruit_deployment_graph_updates()
serve.shutdown()
print("Example ran successfully from the remote repository.")
