# Putting Ray Serve Deployment Graphs in Production

This section should help you:

- test your Serve deployment graph
- create a Serve config file
- deploy, monitor, and update your Serve deployment graph in production

```{contents}
```

## Testing Your Serve Deployment Graph

You can test your Serve deployment graph using the Serve CLI's `serve run` command. The `serve run` command launches a temporary Ray cluster, deploys the graph to it, and blocks. You can then open a new window in the terminal and issue requests to your graph using the Python interpreter. When your graph receives and processes these requests, it will output any `print` or `logging` statements to the terminal. Once your finished testing your graph, you can type `ctrl-C` to kill the temporary Ray cluster and tear down your graph. You can use this pattern to quickly run, debug, and iterate on your Serve deployment graph.

Let's use this graph as an example:

```python
# File name: fruit.py

import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.serve.deployment_graph import InputNode

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

deployment_graph = DAGDriver.bind(net_price, http_adapter=json_resolver)
```

This graph is located in the `fruit.py` file and stored in the `deployment_graph` variable. It takes in requests containing a list of two values: a fruit name and an amount. It returns the total price for the batch of fruits.

To run the deployment graph, we first navigate to the same directory containing the `fruit.py` file and then run `serve run fruit.deployment_graph`. `fruit.deployment_graph` is the deployment graph's import path (assuming we are running `serve run` in the same directory as `fruit.py`).

```console
# Terminal Window 1

$ ls
fruit.py

$ serve run
2022-06-21 13:07:01,966  INFO scripts.py:253 -- Deploying from import path: "fruit.deployment_graph".
2022-06-21 13:07:03,774  INFO services.py:1477 -- View the Ray dashboard at http://127.0.0.1:8265
...
2022-06-21 13:07:08,076  SUCC scripts.py:266 -- Deployed successfully.
```

We can test this graph by opening a new terminal window and making requests with Python's [requests](https://requests.readthedocs.io/en/latest/) library.

```console
# Terminal Window 2

$ python3

>>> import requests
>>> requests.post("http://localhost:8000/", json=["PEAR", 2]).json()
    8
```

Once we're finished, we can close the Python interpreter by running `quit()` and terminate the Ray cluster by typing `ctrl-C` int the terminal running `serve run`. This will tear down the deployments and then the cluster.

## Creating Your Serve Config File

## Deploying Your Serve Application to Production

## Monitoring Your Serve Application in Production

## Updating Your Serve Application in Production
