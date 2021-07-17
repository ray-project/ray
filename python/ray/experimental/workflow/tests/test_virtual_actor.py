import time

import pytest
import ray

from ray.tests.conftest import *  # noqa
from ray.experimental import workflow
from ray.experimental.workflow import virtual_actor


@workflow.actor
class Counter:
    def __init__(self, x: int):
        self.x = x

    def get(self):
        return self.x

    def incr(self):
        self.x += 1
        return self.x

    def workload(self):
        # simulate a workload
        time.sleep(1)

    def __getstate__(self):
        return self.x

    def __setstate__(self, state):
        self.x = state


@workflow.step
def init_virtual_actor(x):
    return x


@pytest.mark.parametrize(
    "ray_start_regular",
    [{
        "namespace": "workflow",
        "num_cpus": 4  # We need more CPUs for concurrency
    }],
    indirect=True)
def test_readonly_actor(ray_start_regular):
    actor = Counter.options(actor_id="Counter").create(42)
    ray.get(actor.ready())
    assert ray.get(actor.get.options(readonly=True).run()) == 42
    assert ray.get(actor.incr.options(readonly=True).run()) == 43
    assert ray.get(actor.get.options(readonly=True).run()) == 42

    # test get actor
    readonly_actor = workflow.get_actor("Counter", readonly=True)
    # test concurrency
    assert ray.get([readonly_actor.get.run() for _ in range(10)]) == [42] * 10
    assert ray.get([readonly_actor.incr.run() for _ in range(10)]) == [43] * 10
    assert ray.get([readonly_actor.get.run() for _ in range(10)]) == [42] * 10
    start = time.time()
    ray.get([readonly_actor.workload.run() for _ in range(10)])
    end = time.time()
    assert end - start < 5


@workflow.actor
class SlowInit:
    def __init__(self, x: int):
        time.sleep(5)
        self.x = x

    def get(self):
        return self.x

    def __getstate__(self):
        return self.x

    def __setstate__(self, state):
        self.x = state


@pytest.mark.parametrize(
    "ray_start_regular",
    [{
        "namespace": "workflow",
        "num_cpus": 4  # We need more CPUs, otherwise 'create()' blocks 'get()'
    }],
    indirect=True)
def test_actor_ready(ray_start_regular):
    actor = SlowInit.options(actor_id="SlowInit").create(42)
    with pytest.raises(virtual_actor.VirtualActorNotInitializedError):
        ray.get(actor.get.options(readonly=True).run())
    ray.get(actor.ready())
    assert ray.get(actor.get.options(readonly=True).run()) == 42
