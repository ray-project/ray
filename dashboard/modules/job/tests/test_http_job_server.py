import sys

import logging
import requests
import pytest
from pytest_lazyfixture import lazy_fixture

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (format_web_url, wait_for_condition,
                                     wait_until_server_available)
from ray._private.job_manager import JobStatus
from ray.dashboard.modules.job.data_types import (
    JobSubmitRequest, JobSubmitResponse, JobStatusRequest, JobStatusResponse,
    JobLogsRequest, JobLogsResponse, JobSpec)

logger = logging.getLogger(__name__)


def _setup_webui_url(ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(webui_url)
    webui_url = format_web_url(webui_url)
    return webui_url


def get_logs(address, job_id) -> JobLogsResponse:
    logs_request = JobLogsRequest(job_id=job_id)
    resp = requests.get(f"{address}/logs", json=logs_request.dict())
    resp.raise_for_status()
    data = resp.json()["data"]["data"]
    return JobLogsResponse(**data)


def get_status(address, job_id) -> JobStatusResponse:
    status_request = JobStatusRequest(job_id=job_id)
    resp = requests.get(f"{address}/status", json=status_request.dict())
    resp.raise_for_status()
    data = resp.json()["data"]["data"]
    return JobStatusResponse(**data)


def submit_job(address, entrypoint, runtime_env=None,
               metadata=None) -> JobSubmitResponse:
    job_spec = JobSpec(
        runtime_env=runtime_env or {},
        entrypoint=entrypoint,
        metadata=metadata or {})

    submit_request = JobSubmitRequest(job_spec=job_spec)
    resp = requests.post(f"{address}/submit", json=submit_request.dict())
    resp.raise_for_status()
    data = resp.json()["data"]["data"]
    return JobSubmitResponse(**data)


def check_job_succeeded(address, job_id):
    resp = get_status(address, job_id)
    if resp.job_status == JobStatus.FAILED:
        logs_resp = get_logs(address, job_id)
        raise RuntimeError("Job failed!\n"
                           f"stdout:\n{logs_resp.stdout}\n"
                           f"stderr:\n{logs_resp.stderr}\n")
    return resp.job_status == JobStatus.SUCCEEDED


@pytest.mark.parametrize(
    "working_dir",
    [lazy_fixture("local_working_dir"),
     lazy_fixture("s3_working_dir")])
def test_submit_job(disable_aiohttp_cache, enable_test_module,
                    ray_start_with_dashboard, working_dir):
    address = _setup_webui_url(ray_start_with_dashboard)

    resp = submit_job(
        address,
        working_dir["entrypoint"],
        runtime_env=working_dir["runtime_env"])
    job_id = resp.job_id

    wait_for_condition(check_job_succeeded, address=address, job_id=job_id)

    resp = get_logs(address, job_id)
    assert resp.stdout == working_dir["expected_stdout"]
    assert resp.stderr == working_dir["expected_stderr"]


def kest_job_metadata(disable_aiohttp_cache, enable_test_module,
                      ray_start_with_dashboard):
    address = _setup_webui_url(ray_start_with_dashboard)

    print_metadata_cmd = (
        "python -c\""
        "import ray;"
        "ray.init();"
        "job_config=ray.worker.global_worker.core_worker.get_job_config();"
        "print(dict(sorted(job_config.metadata.items())))"
        "\"")

    resp = submit_job(
        address, print_metadata_cmd, metadata={
            "key1": "val1",
            "key2": "val2"
        })
    job_id = resp.job_id

    wait_for_condition(check_job_succeeded, address=address, job_id=job_id)

    resp = get_logs(address, job_id)
    assert resp.stdout == "{'key1': 'val1', 'key2': 'val2'}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
