import ray
from ray.experimental.serve import DeadlineAwareRouter
from ray.experimental.serve.examples.adder import VectorizedAdder
from ray.experimental.serve.frontend import HTTPFrontendActor
from ray.experimental.serve.router import start_router
from ray.experimental.serve import RayServeMixin, batched_input
import time
import requests


@ray.remote
class Recommender(RayServeMixin):
    """Actor that returns recommendation.

    """

    def __init__(self, recommendations):
        self.recommendations = recommendations

    def __call__(self):
        return self.recommendations
    def update(self, recommendations):
        self.recommendations = recommendations

@ray.remote
class Aggregator(RayServeMixin):
    """Actor that aggregates model update statistics."""
    def __init__(self):
        self.statistics = []

    def __call__(self, statistics):
        self.statistics += statistics

    def get_statistics(self):
        statistics = self.statistics
        self.statistics = []
        return statistics

@ray.remote
class Trainer:
    """Actor that trains specified actor."""
    def __init__(self, aggregator, model):
        self.aggregator = aggregator
        self.model = model

    def update(self):
        update = self.aggregator.get_statistics.remote()
        self.model.update.remote(update[-1])

ROUTER_NAME = "DefaultRouter"

ray.init()
router = start_router(DeadlineAwareRouter, ROUTER_NAME)
frontend = HTTPFrontendActor.remote(router=ROUTER_NAME)
frontend.start.remote()
router.register_actor.remote(
    "Recommender", Recommender, init_kwargs={"recommendations":[1, 2, 3]}
)
router.register_actor.remote(
    "Aggregator", Aggregator
)
t = Trainer.remote(router.get_actor.remote("Aggregator"), router.get_actor.remote('Recommender'))
urlR = "http://0.0.0.0:8080/Recommender"
urlA = "http://0.0.0.0:8080/Aggregator"
payloadR = {"input":None, "slo_ms": 1000}
payloadA = {"input":[5, 6, 7], "slo_ms":1000}
resp = requests.request("POST", urlR, json=payloadR)
print(resp)
resp = requests.request("POST", urlA, json=payloadA)
print(resp)
t.update.remote()
resp = requests.request("POST", urlR, json=payloadR)
print(resp)
