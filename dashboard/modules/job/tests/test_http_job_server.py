import sys

import logging
import pytest
from pytest_lazyfixture import lazy_fixture

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (format_web_url, wait_for_condition,
                                     wait_until_server_available)
from ray._private.job_manager import JobStatus
from ray.dashboard.modules.job.sdk import JobSubmissionClient

logger = logging.getLogger(__name__)


@pytest.fixture
def job_sdk_client(ray_start_with_dashboard, disable_aiohttp_cache,
                   enable_test_module):
    address = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(address)
    yield JobSubmissionClient(format_web_url(address))


def _check_job_succeeded(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    if status == JobStatus.FAILED:
        stdout, stderr = client.get_job_logs(job_id)
        raise RuntimeError(f"Job failed\nstdout:\n{stdout}\nstderr:\n{stderr}")
    return status == JobStatus.SUCCEEDED


@pytest.mark.parametrize(
    "working_dir",
    [lazy_fixture("local_working_dir"),
     lazy_fixture("s3_working_dir")])
def test_submit_job(job_sdk_client, working_dir):
    client = job_sdk_client

    job_id = client.submit_job(
        entrypoint=working_dir["entrypoint"],
        runtime_env=working_dir["runtime_env"])

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    stdout, stderr = client.get_job_logs(job_id)
    assert stdout == working_dir["expected_stdout"]
    assert stderr == working_dir["expected_stderr"]


def test_job_metadata(job_sdk_client):
    client = job_sdk_client

    print_metadata_cmd = (
        "python -c\""
        "import ray;"
        "ray.init();"
        "job_config=ray.worker.global_worker.core_worker.get_job_config();"
        "print(dict(sorted(job_config.metadata.items())))"
        "\"")

    job_id = client.submit_job(
        entrypoint=print_metadata_cmd,
        metadata={
            "key1": "val1",
            "key2": "val2"
        })

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    stdout, stderr = client.get_job_logs(job_id)
    assert stdout == "{'key1': 'val1', 'key2': 'val2'}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
