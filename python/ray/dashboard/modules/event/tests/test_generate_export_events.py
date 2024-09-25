# isort: skip_file
# flake8: noqa E402
import json
import os
import sys

import pytest

# RAY_enable_export_api_write env var must be set before importing
# `ray` so the correct value is set for RAY_ENABLE_EXPORT_API_WRITE
# even outside a Ray driver.
os.environ["RAY_enable_export_api_write"] = "true"

import ray
from ray._private.gcs_utils import GcsAioClient
from ray._private.test_utils import async_wait_for_condition_async_predicate
from ray.dashboard.modules.job.job_manager import JobManager
from ray.job_submission import JobStatus
from ray.tests.conftest import call_ray_start  # noqa: F401


async def check_job_succeeded(job_manager, job_id):
    data = await job_manager.get_job_info(job_id)
    status = data.status
    if status == JobStatus.FAILED:
        raise RuntimeError(f"Job failed! {data.message}")
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCEEDED}
    if status == JobStatus.SUCCEEDED:
        assert data.driver_exit_code == 0
    else:
        assert data.driver_exit_code is None
    return status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    [
        {
            "env": {
                "RAY_enable_export_api_write": "true",
            },
            "cmd": "ray start --head",
        }
    ],
    indirect=True,
)
async def test_submission_job_export_events(call_ray_start, tmp_path):  # noqa: F811
    """
    Test submission job events are correctly generated and written to file
    as the job goes through various state changes in its lifecycle.
    """

    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    job_manager = JobManager(gcs_aio_client, tmp_path)

    # Submit a job.
    submission_id = await job_manager.submit_job(
        entrypoint="ls",
    )

    # Wait for the job to be finished.
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=submission_id
    )

    # Verify export events are written
    event_dir = f"{tmp_path}/export_events"
    assert os.path.isdir(event_dir)
    event_file = f"{event_dir}/event_EXPORT_SUBMISSION_JOB.log"
    assert os.path.isfile(event_file)

    with open(event_file, "r") as f:
        lines = f.readlines()
        assert len(lines) == 3
        expected_status_values = ["PENDING", "RUNNING", "SUCCEEDED"]

        for line, expected_status in zip(lines, expected_status_values):
            data = json.loads(line)
            assert data["source_type"] == "EXPORT_SUBMISSION_JOB"
            assert data["event_data"]["submission_job_id"] == submission_id
            assert data["event_data"]["status"] == expected_status


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
