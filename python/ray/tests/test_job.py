import os

import ray
from ray.test_utils import (
    run_string_as_driver_nonblocking,
    wait_for_condition,
    wait_for_num_actors,
)


def test_job_gc(call_ray_start):
    address = call_ray_start

    ray.init(address=address)
    driver = """
import ray

ray.init(address="{}")

@ray.remote
class Actor:
    def __init__(self):
        pass

_ = Actor.remote()
""".format(address)

    p = run_string_as_driver_nonblocking(driver)
    # Wait for actor to be created
    wait_for_num_actors(1)

    actor_table = ray.actors()
    assert len(actor_table) == 1

    job_table = ray.jobs()
    assert len(job_table) == 2

    # Kill the driver process.
    p.kill()
    p.wait()

    def actor_finish():
        actor_table = ray.actors()
        if (len(actor_table) == 0):
            return True
        else:
            return False

    wait_for_condition(actor_finish)


def test_job_gc_with_detached_actor(call_ray_start):
    address = call_ray_start

    ray.init(address=address)
    driver = """
import ray

ray.init(address="{}")

@ray.remote
class Actor:
    def __init__(self):
        pass

    def value(self):
        return 1

_ = Actor.options(name="DetachedActor").remote()
""".format(address)

    p = run_string_as_driver_nonblocking(driver)
    # Wait for actor to be created
    wait_for_num_actors(1)

    actor_table = ray.actors()
    assert len(actor_table) == 1

    job_table = ray.jobs()
    assert len(job_table) == 2

    # Kill the driver process.
    p.kill()
    p.wait()

    detached_actor = ray.get_actor("DetachedActor")
    assert ray.get(detached_actor.value.remote()) == 1


if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
