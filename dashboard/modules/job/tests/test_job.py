import os
import sys
import time
import logging
import requests
import traceback

import ray
from ray._private.utils import hex_to_binary
from ray.new_dashboard.tests.conftest import *  # noqa
from ray.test_utils import (
    format_web_url,
    wait_until_server_available,
)
import pytest

os.environ["RAY_USE_NEW_DASHBOARD"] = "1"

logger = logging.getLogger(__name__)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
