import json
import os
import re
import subprocess
import sys
import tempfile
import time
from subprocess import PIPE, STDOUT, Popen, list2cmdline
from typing import List

import pytest

import ray
import ray.cloudpickle as pickle
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    format_web_url,
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_pid_to_exit,
)
from ray.dashboard.modules.job.pydantic_models import JobDetails
from ray.job_config import JobConfig, LoggingConfig
from ray.job_submission import JobStatus, JobSubmissionClient


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


def test_job_isolation(ray_start_regular):
    # Make sure two jobs with same module name
    # don't interfere with each other
    # (https://github.com/ray-project/ray/issues/19358).

    gcs_address = ray_start_regular.address_info["gcs_address"]

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
import os
import sys

# Add current directory to Python path so we can import lib.py
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

import lib

ray.init(address="{}", include_dashboard=True)
assert ray.get(lib.task.remote()) == {}
"""

    def setup_driver_files(base_path: str, version: int) -> str:
        version_path = os.path.join(base_path, f"v{version}")
        os.makedirs(version_path)

        lib_path = os.path.join(version_path, "lib.py")
        driver_path = os.path.join(version_path, "driver.py")

        with open(lib_path, "w") as f:
            f.write(lib_template.format(version))
        with open(driver_path, "w") as f:
            f.write(driver_template.format(gcs_address, version))

        return driver_path

    with tempfile.TemporaryDirectory() as tmpdir:
        v1_driver = setup_driver_files(tmpdir, version=1)
        v2_driver = setup_driver_files(tmpdir, version=2)

        subprocess.check_call([sys.executable, v1_driver])
        subprocess.check_call([sys.executable, v2_driver])

    dashboard_url = ray_start_regular.dashboard_url
    client = JobSubmissionClient(f"http://{dashboard_url}")
    jobs = client.list_jobs()
    assert len(jobs) == 3  # ray_start_regular, v1, v2

    num_succeeded = 0
    num_running = 0
    for job in jobs:
        if job.status == "SUCCEEDED":
            num_succeeded += 1
        elif job.status == "RUNNING":
            num_running += 1
    assert num_succeeded == 2
    assert num_running == 1


def test_job_observability(ray_start_regular):
    driver_template = """
import ray
from time import sleep

ray.init(address="{}", include_dashboard=True)
open("{}", "w+").close()

print("My job id: ", str(ray.get_runtime_context().get_job_id()))

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

    ray.init(job_config=job_config, include_dashboard=True)

    from_worker = ray._private.worker.global_worker.core_worker.get_job_config()

    assert dict(from_worker.metadata) == job_config.metadata


def test_logging_config_serialization():
    logging_config = LoggingConfig(encoding="TEXT")
    serialized_py_logging_config = pickle.dumps(logging_config)
    job_config = JobConfig()
    job_config.set_py_logging_config(logging_config)
    pb = job_config._get_proto_job_config()
    assert pb.serialized_py_logging_config == serialized_py_logging_config


def test_get_entrypoint(tmp_path):
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
    fp = tmp_path / "test.py"
    fp.write_text(get_entrypoint)
    outputs = execute_driver([sys.executable, str(fp), "--flag"])
    assert line_exists(outputs, f"result: {sys.executable} {fp} --flag")

    # Test python shell
    outputs = execute_driver([sys.executable, "-i"], input=get_entrypoint)
    assert line_exists(
        outputs, rf".*result: \(interactive_shell\) {re.escape(sys.executable)} -i.*"
    )

    # Test IPython shell
    outputs = execute_driver(["ipython"], input=get_entrypoint)
    assert line_exists(outputs, r".*result: \(interactive_shell\).*ipython")


def test_removed_internal_flags(shutdown_only):
    ray.init(include_dashboard=True)
    address = ray._private.worker._global_node.webui_url
    address = format_web_url(address)
    client = JobSubmissionClient(address)

    # Tests this env var is not set.
    job_submission_id = client.submit_job(
        entrypoint='[ -z "${RAY_JOB_ID+x}" ] && '
        'echo "RAY_JOB_ID is not set" || '
        '{ echo "RAY_JOB_ID is set to $RAY_JOB_ID"; return 1; }'
    )

    def job_finished():
        status = client.get_job_status(job_submission_id)
        assert status != JobStatus.FAILED
        return status == JobStatus.SUCCEEDED

    wait_for_condition(job_finished)

    all_logs = client.get_job_logs(job_submission_id)
    assert "RAY_JOB_ID is not set" in all_logs


def test_entrypoint_field(shutdown_only, tmp_path):
    """Make sure the entrypoint field is correctly set for jobs."""
    driver = """
import ray
ray.init("auto", include_dashboard=True)

@ray.remote
def f():
    pass

ray.get(f.remote())
"""
    ray.init(include_dashboard=True)
    address = ray._private.worker._global_node.webui_url
    address = format_web_url(address)
    client = JobSubmissionClient(address)

    # Test a regular script.
    fp = tmp_path / "driver.py"
    fp.write_text(driver)

    """
    Test driver.
    """
    commands = [sys.executable, str(fp), "--flag"]
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

    ray.init(include_dashboard=True)

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
    ray.init(address=cluster.address, include_dashboard=True)

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
    sys.exit(pytest.main(["-sv", __file__]))
