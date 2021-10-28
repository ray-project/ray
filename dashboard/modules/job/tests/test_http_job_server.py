import sys

import logging
import requests
from uuid import uuid4
import pytest
from pytest_lazyfixture import lazy_fixture

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (format_web_url,
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


@pytest.mark.parametrize(
    "working_dir",
    [lazy_fixture("local_working_dir"),
     lazy_fixture("s3_working_dir")])
def test_submit_job(disable_aiohttp_cache, enable_test_module,
                    ray_start_with_dashboard, working_dir):
    webui_url = _setup_webui_url(ray_start_with_dashboard)

    job_spec = JobSpec(
        runtime_env=working_dir["runtime_env"],
        entrypoint=working_dir["entrypoint"],
        metadata=dict())
    submit_request = JobSubmitRequest(job_spec=job_spec, job_id=str(uuid4()))

    resp = requests.post(f"{webui_url}/submit", json=submit_request.dict())
    resp.raise_for_status()
    data = resp.json()["data"]["data"]
    response = JobSubmitResponse(**data)
    assert response.job_id == submit_request.job_id

    status_request = JobStatusRequest(job_id=submit_request.job_id)
    resp = requests.get(f"{webui_url}/status", json=status_request.dict())
    resp.raise_for_status()
    data = resp.json()["data"]["data"]
    response = JobStatusResponse(**data)
    assert response.job_status == JobStatus.SUCCEEDED

    logs_request = JobLogsRequest(job_id=submit_request.job_id)
    resp = requests.get(f"{webui_url}/logs", json=logs_request.dict())
    resp.raise_for_status()
    data = resp.json()["data"]["data"]
    response = JobLogsResponse(**data)
    assert response.stdout == working_dir["expected_stdout"]
    assert response.stderr == working_dir["expected_stderr"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
