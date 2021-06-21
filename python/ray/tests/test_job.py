import time
import os

import ray
from ray.job_config import JobConfig
from ray.test_utils import (
    run_string_as_driver,
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

    actor_table = ray.state.actors()
    assert len(actor_table) == 1

    job_table = ray.state.jobs()
    assert len(job_table) == 2  # dash

    # Kill the driver process.
    p.kill()
    p.wait()

    def actor_finish():
        actor_table = ray.state.actors()
        if (len(actor_table) == 0):
            return True
        else:
            return False

    wait_for_condition(actor_finish)


def test_job_gc_with_detached_actor(call_ray_start):
    address = call_ray_start

    ray.init(address=address, namespace="")
    driver = """
import ray

ray.init(address="{}", namespace="")

@ray.remote
class Actor:
    def __init__(self):
        pass

    def value(self):
        return 1

_ = Actor.options(lifetime="detached", name="DetachedActor").remote()
# Make sure the actor is created before the driver exits.
ray.get(_.value.remote())
""".format(address)

    p = run_string_as_driver_nonblocking(driver)
    # Wait for actor to be created
    wait_for_num_actors(1, ray.gcs_utils.ActorTableData.ALIVE)

    actor_table = ray.state.actors()
    assert len(actor_table) == 1

    job_table = ray.state.jobs()
    assert len(job_table) == 2  # dash

    # Kill the driver process.
    p.kill()
    p.wait()

    detached_actor = ray.get_actor("DetachedActor")
    assert ray.get(detached_actor.value.remote()) == 1


def test_job_timestamps(ray_start_regular):
    driver_template = """
import ray
from time import sleep

ray.init(address="{}")

print("My job id: ", str(ray.get_runtime_context().job_id))

{}
ray.shutdown()
    """

    non_hanging = driver_template.format(ray_start_regular["redis_address"],
                                         "sleep(1)")
    hanging_driver = driver_template.format(ray_start_regular["redis_address"],
                                            "sleep(60)")

    out = run_string_as_driver(non_hanging)
    p = run_string_as_driver_nonblocking(hanging_driver)
    # The nonblocking process needs time to connect.
    time.sleep(1)

    jobs = list(ray.state.jobs())
    jobs.sort(key=lambda x: x["JobID"])

    driver = jobs[0]
    finished = jobs[1]
    running = jobs[2]

    # The initial driver timestamp/start time go down a different code path.
    assert driver["Timestamp"] == driver["StartTime"]
    assert finished["Timestamp"] == finished["EndTime"]
    assert running["Timestamp"] == running["StartTime"]

    assert finished["EndTime"] > finished["StartTime"] > 0, out
    lapsed = finished["EndTime"] - finished["StartTime"]
    assert 0 < lapsed < 2000, f"Job should've taken ~1s, {finished}"

    assert running["StartTime"] > 0
    assert running["EndTime"] == 0

    p.kill()
    # Give the second job time to clean itself up.
    time.sleep(1)

    jobs = list(ray.state.jobs())
    jobs.sort(key=lambda x: x["JobID"])

    # jobs[0] is the test case driver.
    finished = jobs[1]
    prev_running = jobs[2]

    assert finished["EndTime"] > finished["StartTime"] > 0, f"{finished}"
    assert finished["EndTime"] == finished["Timestamp"]
    lapsed = finished["EndTime"] - finished["StartTime"]
    assert 0 < lapsed < 2000, f"Job should've taken ~1s {finished}"

    assert prev_running["EndTime"] > prev_running["StartTime"] > 0


def test_config_metadata(shutdown_only):
    job_config = JobConfig(metadata={"abc": "xyz"})
    job_config.set_metadata("xyz", "abc")

    ray.init(job_config=job_config)

    from_worker = ray.worker.global_worker.core_worker.get_job_config()

    assert dict(from_worker.metadata) == job_config.metadata


if __name__ == "__main__":
    import pytest
    import sys
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
