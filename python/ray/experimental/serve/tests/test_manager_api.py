from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import pytest

import ray

from ray.experimental.serve.examples.halt import SleepOnFirst
from ray.experimental.serve.router import DeadlineAwareRouter, start_router
from ray.experimental.serve.object_id import unwrap


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
        "ManagerApiTestActor", SleepOnFirst, init_kwargs={"sleep_time": 1})

    yield handle

    ray.shutdown()


@pytest.fixture
def now():
    return time.perf_counter()

# add test to show performance as a result of batching

def test_change_batch_size(router: DeadlineAwareRouter):

    assert ray.get(router.get_max_batch_size.remote("ManagerApiTestActor")) == -1

    router.set_max_batch_size.remote("ManagerApiTestActor", 10)
    assert ray.get(router.get_max_batch_size.remote("ManagerApiTestActor")) == 10


def test_change_replication_factor(router: DeadlineAwareRouter):

    assert ray.get(router.get_replication_factor.remote("ManagerApiTestActor")) == 1

    router.set_replication_factor.remote("ManagerApiTestActor", 10)
    assert ray.get(router.get_replication_factor.remote("ManagerApiTestActor")) == 10

def test_change_compute_resource(router: DeadlineAwareRouter):

    result = ray.get(router.get_actor_compute_resource.remote("ManagerApiTestActor"))
    assert not 'num_cpus' in result and not 'num_gpus' in result and not 'resources' in result

    kv = {'num_cpus':2}
    router.set_actor_compute_resource.remote("ManagerApiTestActor", kv)
    new_result = ray.get(router.get_actor_compute_resource.remote("ManagerApiTestActor"))
    assert 'num_cpus' in new_result and not 'num_gpus' in new_result and not 'resources' in new_result

    router.reset_compute_resource.remote("ManagerApiTestActor","num_cpus")
    new_result2 = ray.get(router.get_actor_compute_resource.remote("ManagerApiTestActor"))
    assert not 'num_cpus' in new_result2 and not 'num_gpus' in new_result2 and not 'resources' in new_result2
