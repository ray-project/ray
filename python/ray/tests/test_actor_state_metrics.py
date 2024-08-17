import asyncio
from collections import defaultdict
import pytest
import time
from typing import Dict

import ray

from ray.util.state import list_actors
from ray._private.test_utils import (
    raw_metrics,
    wait_for_condition,
    run_string_as_driver,
)
from ray._private.worker import RayContext

_SYSTEM_CONFIG = {
    "metrics_report_interval_ms": 200,
}


def actors_by_state(info: RayContext) -> Dict:
    res = raw_metrics(info)
    actors_info = defaultdict(int)
    if "ray_actors" in res:
        for sample in res["ray_actors"]:
            actors_info[sample.labels["State"]] += sample.value
    for k, v in actors_info.copy().items():
        if v == 0:
            del actors_info[k]
    print(f"Actors by state: {actors_info}")
    return actors_info


def actors_by_name(info: RayContext) -> Dict:
    res = raw_metrics(info)
    actors_info = defaultdict(int)
    if "ray_actors" in res:
        for sample in res["ray_actors"]:
            actors_info[sample.labels["Name"]] += sample.value
    for k, v in actors_info.copy().items():
        if v == 0:
            del actors_info[k]
    print(f"Actors by name: {actors_info}")
    return actors_info


def test_basic_states(shutdown_only):
    info = ray.init(num_cpus=3, _system_config=_SYSTEM_CONFIG)

    @ray.remote(num_cpus=1)
    class Actor:
        def ping(self):
            pass

        def sleep(self):
            time.sleep(999)

        def get(self):
            @ray.remote
            def sleep():
                time.sleep(999)

            ray.get(sleep.remote())

        def wait(self):
            @ray.remote
            def sleep():
                time.sleep(999)

            ray.wait([sleep.remote()])

    a = Actor.remote()
    b = Actor.remote()
    c = Actor.remote()
    ray.get(a.ping.remote())
    ray.get(b.ping.remote())
    ray.get(c.ping.remote())
    d = Actor.remote()

    # Test creation states.
    expected = {
        "ALIVE": 3,
        "IDLE": 3,
        "PENDING_CREATION": 1,
    }
    wait_for_condition(
        lambda: actors_by_state(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )

    # Test running states.
    a.sleep.remote()
    b.get.remote()
    c.wait.remote()
    expected = {
        "ALIVE": 3,
        "RUNNING_TASK": 1,
        "RUNNING_IN_RAY_GET": 1,
        "RUNNING_IN_RAY_WAIT": 1,
        "PENDING_CREATION": 1,
    }
    wait_for_condition(
        lambda: actors_by_state(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )
    del d


def test_destroy_actors(shutdown_only):
    info = ray.init(num_cpus=3, _system_config=_SYSTEM_CONFIG)

    @ray.remote(num_cpus=1)
    class Actor:
        def ping(self):
            pass

    a = Actor.remote()
    b = Actor.remote()
    c = Actor.remote()
    del a
    del b

    expected = {
        "ALIVE": 1,
        "IDLE": 1,
        "DEAD": 2,
    }
    wait_for_condition(
        lambda: actors_by_state(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )
    del c


def test_destroy_actors_from_driver(monkeypatch, shutdown_only):
    with monkeypatch.context() as m:
        # Dead actors are not cached.
        m.setenv("RAY_maximum_gcs_destroyed_actor_cached_count", 5)
        m.setenv("RAY_WORKER_TIMEOUT_S", 5)
        driver = """
import ray
ray.init("auto")
@ray.remote(num_cpus=0)
class Actor:
    def ready(self):
        pass
actors = [Actor.remote() for _ in range(10)]
ray.get([actor.ready.remote() for actor in actors])
"""
        info = ray.init(num_cpus=3, _system_config=_SYSTEM_CONFIG)

        output = run_string_as_driver(driver)
        print(output)

        expected = {
            "DEAD": 10,
        }
        wait_for_condition(
            lambda: actors_by_state(info) == expected,
            timeout=20,
            retry_interval_ms=500,
        )

        """
        Make sure even after the actor entries are deleted from GCS by GC
        the metrics are correct.
        """
        # Wait until the state API returns the # of actors are 0
        # becasue entries are GC'ed by GCS.
        wait_for_condition(lambda: len(list_actors()) == 5)
        # DEAD count shouldn't be changed.
        wait_for_condition(
            lambda: actors_by_state(info) == expected,
            timeout=20,
            retry_interval_ms=500,
        )


def test_dep_wait(shutdown_only):
    info = ray.init(num_cpus=3, _system_config=_SYSTEM_CONFIG)

    @ray.remote
    def sleep():
        time.sleep(999)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self, x):
            pass

    a = Actor.remote(sleep.remote())
    expected = {
        "DEPENDENCIES_UNREADY": 1,
    }
    wait_for_condition(
        lambda: actors_by_state(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )
    del a


def test_async_actor(shutdown_only):
    info = ray.init(num_cpus=3, _system_config=_SYSTEM_CONFIG)

    @ray.remote
    def sleep():
        time.sleep(999)

    @ray.remote(max_concurrency=30)
    class AsyncActor:
        async def sleep(self):
            await asyncio.sleep(300)

        async def do_get(self):
            await ray.get(sleep.remote())

    a = AsyncActor.remote()
    a.sleep.remote()
    expected = {
        "ALIVE": 1,
        "RUNNING_TASK": 1,
    }
    wait_for_condition(
        lambda: actors_by_state(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )

    # Test that this transitions the entire actor to reporting IN_RAY_GET state.
    a.do_get.remote()
    a.do_get.remote()
    expected = {
        "ALIVE": 1,
        "RUNNING_IN_RAY_GET": 1,
    }
    wait_for_condition(
        lambda: actors_by_state(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )
    del a


@ray.remote(num_cpus=1)
class Actor1:
    def sleep(self):
        time.sleep(999)


@ray.remote(num_cpus=1)
class Actor2:
    def sleep(self):
        time.sleep(999)


def test_tracking_by_name(shutdown_only):
    info = ray.init(num_cpus=3, _system_config=_SYSTEM_CONFIG)

    a = Actor1.remote()
    b = Actor2.remote()

    expected = {
        # one reported by gcs as ALIVE
        # another reported by core worker as IDLE
        "Actor1": 2,
        "Actor2": 2,
    }
    wait_for_condition(
        lambda: actors_by_name(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )

    del a
    del b


def test_get_all_actors_info(shutdown_only):
    ray.init(num_cpus=2)

    @ray.remote(num_cpus=1)
    class Actor:
        def ping(self):
            pass

    actor_1 = Actor.remote()
    actor_2 = Actor.remote()
    ray.get([actor_1.ping.remote(), actor_2.ping.remote()], timeout=5)
    actors_info = ray.state.actors()
    assert len(actors_info) == 2

    # To filter actors by job id
    job_id = ray.get_runtime_context().job_id
    actors_info = ray.state.actors(job_id=job_id)
    assert len(actors_info) == 2
    actors_info = ray.state.actors(job_id=ray.JobID.from_int(100))
    assert len(actors_info) == 0

    # To filter actors by state
    actor_3 = Actor.remote()
    wait_for_condition(
        lambda: len(ray.state.actors(actor_state_name="PENDING_CREATION")) == 1
    )
    assert (
        actor_3._actor_id.hex()
        in ray.state.actors(actor_state_name="PENDING_CREATION").keys()
    )

    with pytest.raises(ValueError, match="not a valid actor state name"):
        actors_info = ray.state.actors(actor_state_name="UNKONWN_STATE")


if __name__ == "__main__":
    import sys
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
