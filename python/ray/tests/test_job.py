import time
import os
import sys
import tempfile
import subprocess

import ray
from ray.job_config import JobConfig
import ray._private.gcs_utils as gcs_utils
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
    wait_for_num_actors,
)


def test_job_isolation(call_ray_start):
    # Make sure two jobs with same module name
    # don't interfere with each other
    # (https://github.com/ray-project/ray/issues/19358).
    address = call_ray_start
    lib_template = """
import ray

@ray.remote
def task():
    return subtask()

def subtask():
    return {}
"""
    driver_template = """
import ray
import lib

ray.init(address="{}")
assert ray.get(lib.task.remote()) == {}
"""
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(os.path.join(tmpdir, "v1"))
        v1_lib = os.path.join(tmpdir, "v1", "lib.py")
        v1_driver = os.path.join(tmpdir, "v1", "driver.py")
        with open(v1_lib, "w") as f:
            f.write(lib_template.format(1))
        with open(v1_driver, "w") as f:
            f.write(driver_template.format(address, 1))

        os.makedirs(os.path.join(tmpdir, "v2"))
        v2_lib = os.path.join(tmpdir, "v2", "lib.py")
        v2_driver = os.path.join(tmpdir, "v2", "driver.py")
        with open(v2_lib, "w") as f:
            f.write(lib_template.format(2))
        with open(v2_driver, "w") as f:
            f.write(driver_template.format(address, 2))

        subprocess.check_call([sys.executable, v1_driver])
        subprocess.check_call([sys.executable, v2_driver])


def test_export_queue_isolation(call_ray_start):
    address = call_ray_start
    driver_template = """
import ray
import ray.experimental.internal_kv as kv
ray.init(address="{}")

@ray.remote
def f():
    pass

ray.get(f.remote())

count = 0
for k in kv._internal_kv_list(""):
    if b"IsolatedExports:" + ray.get_runtime_context().job_id.binary() in k:
        count += 1

# Check exports aren't shared across the 5 jobs.
assert count < 5, count
"""
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(os.path.join(tmpdir, "v1"))
        v1_driver = os.path.join(tmpdir, "v1", "driver.py")
        with open(v1_driver, "w") as f:
            f.write(driver_template.format(address))

        try:
            subprocess.check_call([sys.executable, v1_driver])
        except Exception:
            # Ignore the first run, since it runs extra exports.
            pass

        # Further runs do not increase the num exports count.
        for _ in range(5):
            subprocess.check_call([sys.executable, v1_driver])


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

    ray.init(address=address, namespace="test")
    driver = """
import ray

ray.init(address="{}", namespace="test")

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
    wait_for_num_actors(1, gcs_utils.ActorTableData.ALIVE)

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

    non_hanging = driver_template.format(ray_start_regular["address"],
                                         "sleep(1)")
    hanging_driver = driver_template.format(ray_start_regular["address"],
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
    assert 0 < lapsed < 5000, f"Job should've taken ~1s, {finished}"

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
    assert 0 < lapsed < 5000, f"Job should've taken ~1s {finished}"

    assert prev_running["EndTime"] > prev_running["StartTime"] > 0


def test_config_metadata(shutdown_only):
    job_config = JobConfig(metadata={"abc": "xyz"})
    job_config.set_metadata("xyz", "abc")

    ray.init(job_config=job_config)

    from_worker = ray.worker.global_worker.core_worker.get_job_config()

    assert dict(from_worker.metadata) == job_config.metadata


if __name__ == "__main__":
    import pytest
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
