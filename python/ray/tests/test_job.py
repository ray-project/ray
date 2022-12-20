import os
import subprocess
import sys
import tempfile
import time
import re
import json

from subprocess import Popen, PIPE, STDOUT, list2cmdline
from typing import List
from pathlib import Path

import ray
import ray._private.gcs_utils as gcs_utils
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
    wait_for_num_actors,
    format_web_url,
)
from ray.job_config import JobConfig
from ray.job_submission import JobSubmissionClient
from ray.dashboard.modules.job.pydantic_models import JobDetails


def execute_driver(commands: List[str], input: bytes = None):
    p = None
    outs = []
    try:
        p = Popen(commands, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        if isinstance(input, str):
            input = input.encode()

        stdout, stderr = p.communicate(input=input)
        outs = stdout.decode().split("\n")
        return outs
    finally:
        if p:
            try:
                p.kill()
            except Exception:
                pass


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
""".format(
        address
    )

    p = run_string_as_driver_nonblocking(driver)
    # Wait for actor to be created
    wait_for_num_actors(1)

    actor_table = ray._private.state.actors()
    assert len(actor_table) == 1

    job_table = ray._private.state.jobs()
    assert len(job_table) == 2  # dash

    # Kill the driver process.
    p.kill()
    p.wait()

    def actor_finish():
        actor_table = ray._private.state.actors()
        if len(actor_table) == 0:
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
""".format(
        address
    )

    p = run_string_as_driver_nonblocking(driver)
    # Wait for actor to be created
    wait_for_num_actors(1, gcs_utils.ActorTableData.ALIVE)

    actor_table = ray._private.state.actors()
    assert len(actor_table) == 1

    job_table = ray._private.state.jobs()
    assert len(job_table) == 2  # dash

    # Kill the driver process.
    p.kill()
    p.wait()

    detached_actor = ray.get_actor("DetachedActor")
    assert ray.get(detached_actor.value.remote()) == 1


def test_job_observability(ray_start_regular):
    driver_template = """
import ray
from time import sleep

ray.init(address="{}")
open("{}", "w+").close()

print("My job id: ", str(ray.get_runtime_context().job_id))

{}
ray.shutdown()
    """
    tmpfile1 = tempfile.NamedTemporaryFile("w+", suffix=".tmp", prefix="_")
    tmpfile2 = tempfile.NamedTemporaryFile("w+", suffix=".tmp", prefix="_")
    tmpfiles = [tmpfile1.name, tmpfile2.name]
    tmpfile1.close()
    tmpfile2.close()
    for tmpfile in tmpfiles:
        if os.path.exists(tmpfile):
            os.unlink(tmpfile)

    non_hanging = driver_template.format(
        ray_start_regular["address"], tmpfiles[0], "sleep(1)"
    )
    hanging_driver = driver_template.format(
        ray_start_regular["address"], tmpfiles[1], "sleep(60)"
    )

    out = run_string_as_driver(non_hanging)
    p = run_string_as_driver_nonblocking(hanging_driver)
    # The nonblocking process needs time to connect.
    while not os.path.exists(tmpfiles[1]):
        time.sleep(1)

    jobs = list(ray._private.state.jobs())
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

    assert len(running["DriverIPAddress"]) > 0
    assert running["DriverPid"] > 0
    assert len(finished["DriverIPAddress"]) > 0
    assert finished["DriverPid"] > 0

    p.kill()
    # Give the second job time to clean itself up.
    time.sleep(1)

    jobs = list(ray._private.state.jobs())
    jobs.sort(key=lambda x: x["JobID"])

    # jobs[0] is the test case driver.
    finished = jobs[1]
    prev_running = jobs[2]

    assert finished["EndTime"] > finished["StartTime"] > 0, f"{finished}"
    assert finished["EndTime"] == finished["Timestamp"]
    lapsed = finished["EndTime"] - finished["StartTime"]
    assert 0 < lapsed < 5000, f"Job should've taken ~1s {finished}"

    assert prev_running["EndTime"] > prev_running["StartTime"] > 0

    assert len(prev_running["DriverIPAddress"]) > 0
    assert prev_running["DriverPid"] > 0


def test_config_metadata(shutdown_only):
    job_config = JobConfig(metadata={"abc": "xyz"})
    job_config.set_metadata("xyz", "abc")

    ray.init(job_config=job_config)

    from_worker = ray._private.worker.global_worker.core_worker.get_job_config()

    assert dict(from_worker.metadata) == job_config.metadata


def test_get_entrypoint():
    get_entrypoint = """
from ray._private.utils import get_entrypoint_name
print("result:", get_entrypoint_name())
"""

    def line_exists(lines: List[str], regex_target: str):
        p = re.compile(regex_target)
        for line in lines:
            m = p.match(line.strip(" \n"))
            if m:
                return True
        print(f"No target {regex_target} in lines {lines}")
        return False

    # Test a regular script.
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(get_entrypoint.encode())
        fp.seek(0)
        path = Path(fp.name)
        outputs = execute_driver(["python", str(path), "--flag"])
        assert line_exists(outputs, f"result: python {path} --flag")

    # Test python shell
    outputs = execute_driver(["python", "-i"], input=get_entrypoint)
    assert line_exists(outputs, ".*result: \(interactive_shell\) python -i.*")

    # Test IPython shell
    outputs = execute_driver(["ipython"], input=get_entrypoint)
    assert line_exists(outputs, ".*result: \(interactive_shell\).*ipython")


def test_entrypoint_field(shutdown_only):
    """Make sure the entrypoint field is correctly set for jobs."""
    driver = """
import ray
ray.init("auto")

@ray.remote
def f():
    pass

ray.get(f.remote())
"""
    ray.init()
    address = ray._private.worker._global_node.webui_url
    address = format_web_url(address)
    client = JobSubmissionClient(address)

    # Test a regular script.
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(driver.encode())
        fp.seek(0)
        path = Path(fp.name)

        """
        Test driver.
        """
        commands = ["python", str(path), "--flag"]
        print(execute_driver(commands))

        jobs = ray.state.jobs()
        assert len(jobs) == 2
        jobs = list(jobs)
        jobs.sort(key=lambda j: j["JobID"])

        # The first job is the test job.

        driver_job = jobs[1]
        assert driver_job["Entrypoint"] == list2cmdline(commands)

        # Make sure the Dashboard endpoint works
        r = client._do_request(
            "GET",
            "/api/jobs/",
        )

        assert r.status_code == 200
        jobs_info_json = json.loads(r.text)
        jobs_info_json.sort(key=lambda j: j["job_id"])
        info_json = jobs_info_json[1]
        info = JobDetails(**info_json)
        assert info.entrypoint == list2cmdline(commands)

        """
        Test job submission
        """
        client.submit_job(entrypoint=list2cmdline(commands))

        def verify():
            jobs = ray.state.jobs()
            # Test, first job, agent, submission job
            assert len(jobs) == 4
            jobs = list(jobs)
            jobs.sort(key=lambda j: j["JobID"])

            # The first job is the test job.

            submission_job = jobs[3]
            assert submission_job["Entrypoint"] == list2cmdline(commands)
            return True

        wait_for_condition(verify)

        # Test client
        # TODO(sang): Client entrypoint not supported yet.


if __name__ == "__main__":
    import pytest

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
