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
import pytest

import ray.cloudpickle as pickle

import ray
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
    format_web_url,
    wait_for_pid_to_exit,
)
from ray.job_config import JobConfig, LoggingConfig
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


def test_invalid_gcs_address():
    with pytest.raises(ValueError):
        JobSubmissionClient("foobar")

    with pytest.raises(ValueError):
        JobSubmissionClient("")

    with pytest.raises(ValueError):
        JobSubmissionClient("abc:abc")


@pytest.mark.skipif(
    sys.version_info >= (3, 12), reason="lib is not supported on Python 3.12"
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


def test_logging_config_serialization():
    logging_config = LoggingConfig(encoding="TEXT")
    serialized_py_logging_config = pickle.dumps(logging_config)
    job_config = JobConfig()
    job_config.set_py_logging_config(logging_config)
    pb = job_config._get_proto_job_config()
    assert pb.serialized_py_logging_config == serialized_py_logging_config


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

        assert r.status_code == 200, r.text
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


def test_task_spec_root_detached_actor_id(shutdown_only):
    """Test to make sure root detached actor id is set correctly
    for task spec of submitted task or actor.
    """

    ray.init()

    @ray.remote
    def get_task_root_detached_actor_id():
        core_worker = ray._private.worker.global_worker.core_worker
        return core_worker.get_current_root_detached_actor_id().hex()

    @ray.remote
    class Actor:
        def get_root_detached_actor_id(self):
            core_worker = ray._private.worker.global_worker.core_worker
            return core_worker.get_current_root_detached_actor_id().hex()

    @ray.remote(lifetime="detached")
    class DetachedActor:
        def check(self):
            core_worker = ray._private.worker.global_worker.core_worker
            assert (
                ray.get_runtime_context().get_actor_id()
                == core_worker.get_current_root_detached_actor_id().hex()
            )
            assert ray.get_runtime_context().get_actor_id() == ray.get(
                get_task_root_detached_actor_id.remote()
            )
            actor = Actor.remote()
            assert ray.get_runtime_context().get_actor_id() == ray.get(
                actor.get_root_detached_actor_id.remote()
            )

    assert (
        ray.get(get_task_root_detached_actor_id.remote())
        == ray._raylet.ActorID.nil().hex()
    )
    actor = Actor.remote()
    assert (
        ray.get(actor.get_root_detached_actor_id.remote())
        == ray._raylet.ActorID.nil().hex()
    )
    detached_actor = DetachedActor.remote()
    ray.get(detached_actor.check.remote())


def test_no_process_leak_after_job_finishes(ray_start_cluster):
    """Test to make sure when a job finishes,
    all the worker processes belonging to it exit.
    """
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=8)
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0)
    class PidActor:
        def __init__(self):
            self.pids = set()
            self.pids.add(os.getpid())

        def add_pid(self, pid):
            self.pids.add(pid)

        def get_pids(self):
            return self.pids

    @ray.remote
    def child(pid_actor):
        # child worker process should be forcibly killed
        # when the job finishes.
        ray.get(pid_actor.add_pid.remote(os.getpid()))
        time.sleep(1000000)

    @ray.remote
    def parent(pid_actor):
        ray.get(pid_actor.add_pid.remote(os.getpid()))
        child.remote(pid_actor)

    pid_actor = PidActor.remote()
    ray.get(parent.remote(pid_actor))

    wait_for_condition(lambda: len(ray.get(pid_actor.get_pids.remote())) == 3)

    pids = ray.get(pid_actor.get_pids.remote())

    ray.shutdown()
    # Job finishes at this point

    for pid in pids:
        wait_for_pid_to_exit(pid)


if __name__ == "__main__":

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
