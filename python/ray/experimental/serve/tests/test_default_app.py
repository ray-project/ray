from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import pytest
import requests

import ray
from ray.experimental.serve import DeadlineAwareRouter
from ray.experimental.serve.examples.adder import VectorizedAdder
from ray.experimental.serve.frontend import HTTPFrontendActor
from ray.experimental.serve.router import start_router

ROUTER_NAME = "DefaultRouter"
NUMBER_OF_TRIES = 5


@pytest.fixture
def get_router():
    # We need this many workers so resource are not oversubscribed
    ray.init(num_cpus=4)
    router = start_router(DeadlineAwareRouter, ROUTER_NAME)
    yield router
    ray.shutdown()


def test_http_basic(get_router):
    router = get_router
    a = HTTPFrontendActor.remote(router=ROUTER_NAME)
    a.start.remote()

    router.register_actor.remote(
        "VAdder", VectorizedAdder, init_kwargs={"scaler_increment": 1})

    for _ in range(NUMBER_OF_TRIES):
        try:
            url = "http://0.0.0.0:8080/VAdder"
            payload = {"input": 10, "slo_ms": 1000}
            resp = requests.request("POST", url, json=payload)
        except Exception:
            # it is possible that the actor is not yet instantiated
            time.sleep(1)

    assert resp.json() == {"success": True, "actor": "VAdder", "result": 11}
