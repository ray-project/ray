import asyncio
from collections import defaultdict
import pytest
import time
from typing import Dict

import ray

from ray.experimental.state.api import list_actors
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

    # Test the GCS recorded case.
    expected = {
        "Actor1": 1,
        "Actor2": 1,
    }
    wait_for_condition(
        lambda: actors_by_name(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )

    # Also test the core worker recorded case.
    a.sleep.remote()
    b.sleep.remote()
    time.sleep(1)
    wait_for_condition(
        lambda: actors_by_name(info) == expected,
        timeout=20,
        retry_interval_ms=500,
    )


if __name__ == "__main__":
    import sys
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
