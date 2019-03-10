from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import numpy as np
import pytest

import ray
from ray.experimental.serve.examples.adder import ScalerAdder, VectorizedAdder
from ray.experimental.serve.examples.halt import SleepCounter, SleepOnFirst
from ray.experimental.serve.object_id import unwrap
from ray.experimental.serve.router import DeadlineAwareRouter, start_router


@pytest.fixture(scope="module")
def router():
    # We need at least 5 workers so resource won't be oversubscribed
    ray.init(num_cpus=5)

    # The following two blobs are equivalent
    #
    # handle = DeadlineAwareRouter.remote("DefaultTestRouter")
    # ray.experimental.register_actor("DefaultTestRouter", handle)
    # handle.start.remote()
    #
    # handle = start_router(DeadlineAwareRouter, "DefaultRouter")
    handle = start_router(DeadlineAwareRouter, "DefaultRouter")

    handle.register_actor.remote(
        "VAdder", VectorizedAdder,
        init_kwargs={"scaler_increment": 1})  # init args
    handle.register_actor.remote(
        "SAdder", ScalerAdder, init_kwargs={"scaler_increment": 2})
    handle.register_actor.remote(
        "SleepFirst", SleepOnFirst, init_kwargs={"sleep_time": 1})
    handle.register_actor.remote(
        "SleepCounter", SleepCounter, max_batch_size=1)

    yield handle

    ray.shutdown()


@pytest.fixture
def now():
    return time.perf_counter()


def test_throw_assert(router: DeadlineAwareRouter, now: float):
    try:
        ray.get(router.call.remote("Action-Not-Exist", "input", now + 1))
    except ray.worker.RayTaskError as e:
        assert "AssertionError" in e.traceback_str


def test_vector_adder(router: DeadlineAwareRouter, now: float):
    result = unwrap(router.call.remote("VAdder", 42, now + 1))
    assert isinstance(result, ray.ObjectID)
    assert ray.get(result) == 43


def test_scaler_adder(router: DeadlineAwareRouter, now: float):
    result = unwrap(router.call.remote("SAdder", 42, now + 1))
    assert isinstance(result, ray.ObjectID)
    assert ray.get(result) == 44


def test_batching_ability(router: DeadlineAwareRouter, now: float):
    first = unwrap(router.call.remote("SleepFirst", 1, now + 1))
    rest = [
        unwrap(router.call.remote("SleepFirst", 1, now + 1)) for _ in range(10)
    ]
    assert ray.get(first) == 1
    assert np.alltrue(np.array(ray.get(rest)) == 10)


def test_deadline_priority(router: DeadlineAwareRouter, now: float):
    # first sleep 2 seconds
    first = unwrap(router.call.remote("SleepCounter", 2, now + 1))

    # then send a request to with deadline farther away
    second = unwrap(router.call.remote("SleepCounter", 0, now + 10))

    # and a request with sooner deadline
    third = unwrap(router.call.remote("SleepCounter", 0, now + 1))

    id_1, id_2, id_3 = ray.get([first, second, third])

    assert id_1 < id_3 < id_2
