from time import perf_counter

import pytest
import ray
from ray.serve import DeadlineAwareRouter, unwrap
from ray.serve.examples import VectorizedAdder
from ray.serve.router import start_router


@pytest.fixture(scope="module")
def router():
    ray.init()

    # handle = DeadlineAwareRouter.remote("DefaultTestRouter")
    # ray.experimental.register_actor("DefaultTestRouter", handle)
    # handle.start.remote()
    handle = start_router(DeadlineAwareRouter, "DefaultRouter")

    handle.register_actor.remote("VAdder", VectorizedAdder, [1])  # init args
    return handle


@pytest.fixture
def now():
    return perf_counter()


def test_throw_assert(router: DeadlineAwareRouter, now: float):
    try:
        ray.get(router.call.remote("Action-Not-Exist", "input", now + 1))
    except ray.worker.RayTaskError as e:
        assert "AssertionError" in e.traceback_str


def test_basic_adder(router: DeadlineAwareRouter, now: float):
    result = unwrap(router.call.remote("VAdder", 42, now + 1))
    assert isinstance(result, ray.ObjectID)
    assert ray.get(result) == 43
