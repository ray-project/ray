import os
import sys
import copy
import time
import logging
import requests
import tempfile
import zipfile
import shutil
import traceback

import ray
from ray._private.utils import hex_to_binary
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.new_dashboard.modules.job import job_consts
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
    wait_until_succeeded_without_exception,
)
import pytest

logger = logging.getLogger(__name__)

TEST_PYTHON_JOB = {
    "language": job_consts.PYTHON,
    "runtime_env": {
        "working_dir": "{web_url}/test/file?path={path}"
    },
    "driver_entry": "simple_job",
}

TEST_PYTHON_JOB_CODE = """
import os
import sys
import ray
import time


@ray.remote
class Actor:
    def __init__(self, index):
        self._index = index

    def foo(self, x):
        return f"Actor {self._index}: {x}"


def main():
    actors = []
    for x in range(2):
        actors.append(Actor.remote(x))

    counter = 0
    while True:
        for a in actors:
            r = a.foo.remote(counter)
            print(ray.get(r))
            counter += 1
            time.sleep(1)


if __name__ == "__main__":
    ray.init()
    main()
"""


def _gen_job_zip(job_code, driver_entry):
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        with zipfile.ZipFile(f, mode="w") as zip_f:
            with zip_f.open(f"{driver_entry}.py", "w") as driver:
                driver.write(job_code.encode())
        return f.name


def _prepare_job_for_test(web_url):
    path = _gen_job_zip(TEST_PYTHON_JOB_CODE, TEST_PYTHON_JOB["driver_entry"])
    job = copy.deepcopy(TEST_PYTHON_JOB)
    job["runtime_env"]["working_dir"] = job["runtime_env"][
        "working_dir"].format(
            web_url=web_url, path=path)
    return job


@pytest.mark.parametrize(
    "ray_start_with_dashboard", [{
        "job_config": ray.job_config.JobConfig(code_search_path=[""]),
    }],
    indirect=True)
def test_submit_job(disable_aiohttp_cache, enable_test_module,
                    ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    job = _prepare_job_for_test(webui_url)
    job_root_dir = os.path.join(
        os.path.dirname(ray_start_with_dashboard["session_dir"]), "job")
    shutil.rmtree(job_root_dir, ignore_errors=True)

    job_id = None
    job_submitted = False

    def _check_running():
        nonlocal job_id
        nonlocal job_submitted
        if not job_submitted:
            resp = requests.post(f"{webui_url}/jobs", json=job)
            resp.raise_for_status()
            result = resp.json()
            assert result["result"] is True, resp.text
            job_submitted = True

        resp = requests.get(f"{webui_url}/jobs?view=summary")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        summary = result["data"]["summary"]
        assert len(summary) == 2

        # TODO(fyrestone): Return a job id when POST /jobs
        # The larger job id is the one we submitted.
        job_ids = sorted(s["jobId"] for s in summary)
        job_id = job_ids[1]

        resp = requests.get(f"{webui_url}/jobs/{job_id}")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_info = result["data"]["detail"]["jobInfo"]
        assert job_info["jobId"] == job_id

        resp = requests.get(f"{webui_url}/jobs/{job_id}")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_info = result["data"]["detail"]["jobInfo"]
        assert job_info["isDead"] is False
        job_actors = result["data"]["detail"]["jobActors"]
        job_workers = result["data"]["detail"]["jobWorkers"]
        assert len(job_actors) > 0
        assert len(job_workers) > 0

    wait_until_succeeded_without_exception(
        _check_running,
        exceptions=(AssertionError, KeyError, IndexError),
        timeout_ms=30 * 1000,
        raise_last_ex=True)


def test_get_job_info(disable_aiohttp_cache, ray_start_with_dashboard):
    @ray.remote
    class Actor:
        def getpid(self):
            return os.getpid()

    actor = Actor.remote()
    actor_pid = ray.get(actor.getpid.remote())
    actor_id = actor._actor_id.hex()

    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    ip = ray.util.get_node_ip_address()

    def _check():
        resp = requests.get(f"{webui_url}/jobs?view=summary")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_summary = result["data"]["summary"]
        assert len(job_summary) == 1, resp.text
        one_job = job_summary[0]
        assert "jobId" in one_job
        job_id = one_job["jobId"]
        assert ray._raylet.JobID(hex_to_binary(one_job["jobId"]))
        assert "driverIpAddress" in one_job
        assert one_job["driverIpAddress"] == ip
        assert "driverPid" in one_job
        assert one_job["driverPid"] == str(os.getpid())
        assert "config" in one_job
        assert type(one_job["config"]) is dict
        assert "isDead" in one_job
        assert one_job["isDead"] is False
        assert "timestamp" in one_job
        one_job_summary_keys = one_job.keys()

        resp = requests.get(f"{webui_url}/jobs/{job_id}")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is True, resp.text
        job_detail = result["data"]["detail"]
        assert "jobInfo" in job_detail
        assert len(one_job_summary_keys - job_detail["jobInfo"].keys()) == 0
        assert "jobActors" in job_detail
        job_actors = job_detail["jobActors"]
        assert len(job_actors) == 1, resp.text
        one_job_actor = job_actors[actor_id]
        assert "taskSpec" in one_job_actor
        assert type(one_job_actor["taskSpec"]) is dict
        assert "functionDescriptor" in one_job_actor["taskSpec"]
        assert type(one_job_actor["taskSpec"]["functionDescriptor"]) is dict
        assert "pid" in one_job_actor
        assert one_job_actor["pid"] == actor_pid
        check_actor_keys = [
            "name", "timestamp", "address", "actorId", "jobId", "state"
        ]
        for k in check_actor_keys:
            assert k in one_job_actor
        assert "jobWorkers" in job_detail
        job_workers = job_detail["jobWorkers"]
        assert len(job_workers) == 1, resp.text
        one_job_worker = job_workers[0]
        check_worker_keys = [
            "cmdline", "pid", "cpuTimes", "memoryInfo", "cpuPercent",
            "coreWorkerStats", "language", "jobId"
        ]
        for k in check_worker_keys:
            assert k in one_job_worker

    timeout_seconds = 30
    start_time = time.time()
    last_ex = None
    while True:
        time.sleep(5)
        try:
            _check()
            break
        except (AssertionError, KeyError, IndexError) as ex:
            last_ex = ex
        finally:
            if time.time() > start_time + timeout_seconds:
                ex_stack = traceback.format_exception(
                    type(last_ex), last_ex,
                    last_ex.__traceback__) if last_ex else []
                ex_stack = "".join(ex_stack)
                raise Exception(f"Timed out while testing, {ex_stack}")


def test_submit_job_validation(ray_start_with_dashboard):
    assert (wait_until_server_available(ray_start_with_dashboard["webui_url"])
            is True)
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    job_root_dir = os.path.join(
        os.path.dirname(ray_start_with_dashboard["session_dir"]), "job")
    shutil.rmtree(job_root_dir, ignore_errors=True)

    def _ensure_available_nodes():
        resp = requests.post(f"{webui_url}/jobs")
        resp.raise_for_status()
        result = resp.json()
        assert result["result"] is False
        return "no nodes available" not in result["msg"]

    wait_for_condition(_ensure_available_nodes, timeout=5)

    # Invalid value.
    resp = requests.post(
        f"{webui_url}/jobs",
        json={
            "language": "Unsupported",
            "runtime_env": {
                "working_dir": "http://xxx/yyy.zip"
            },
            "driver_entry": "python_file_name_without_ext",
        })
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is False
    msg = result["msg"]
    assert "language" in msg and "Unsupported" in msg, resp.text

    # Missing required field.
    resp = requests.post(
        f"{webui_url}/jobs",
        json={
            "language": job_consts.PYTHON,
            "runtime_env": {
                "working_dir": "http://xxx/yyy.zip"
            },
        })
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is False
    msg = result["msg"]
    assert all(p in msg for p in ["missing", "driver_entry"]), resp.text

    # Incorrect value type.
    resp = requests.post(
        f"{webui_url}/jobs",
        json={
            "language": job_consts.PYTHON,
            "runtime_env": {
                "working_dir": ["http://xxx/yyy.zip"]
            },
            "driver_entry": "python_file_name_without_ext",
        })
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is False
    msg = result["msg"]
    assert all(p in msg for p in ["working_dir", "str"]), resp.text

    # Invalid key.
    resp = requests.post(
        f"{webui_url}/jobs",
        json={
            "language": job_consts.PYTHON,
            "runtime_env": {
                "working_dir": "http://xxx/yyy.zip"
            },
            "driver_entry": "python_file_name_without_ext",
            "invalid_key": 1,
        })
    resp.raise_for_status()
    result = resp.json()
    assert result["result"] is False
    msg = result["msg"]
    assert all(p in msg for p in ["unexpected", "invalid_key"]), resp.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
