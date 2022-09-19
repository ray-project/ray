import os
import shutil
import sys
import tempfile

import pytest

from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.job_submission import JobStatus, JobSubmissionClient
from ray.tests.conftest import _ray_start


@pytest.fixture(scope="module")
def headers():
    return {"Connection": "keep-alive", "Authorization": "TOK:<MY_TOKEN>"}


@pytest.fixture(scope="module")
def job_sdk_client(headers):
    with _ray_start(include_dashboard=True, num_cpus=1) as ctx:
        address = ctx.address_info["webui_url"]
        assert wait_until_server_available(address)
        yield JobSubmissionClient(format_web_url(address), headers=headers)


def _check_job_succeeded(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    if status == JobStatus.FAILED:
        logs = client.get_job_logs(job_id)
        raise RuntimeError(f"Job failed\nlogs:\n{logs}")
    return status == JobStatus.SUCCEEDED


def test_submit_simple_java_job(job_sdk_client):
    client = job_sdk_client

    simple_job_jar_path = os.environ["SIMPLE_JOB_JAR_PATH"]
    simple_job_jar_filename = os.path.basename(simple_job_jar_path)
    with tempfile.TemporaryDirectory() as tmp_dir:
        working_dir = os.path.join(tmp_dir, "java_worker")
        os.makedirs(working_dir)
        shutil.copy2(
            simple_job_jar_path, os.path.join(working_dir, simple_job_jar_filename)
        )

        entrypoint = f"java -cp  {simple_job_jar_filename} io.ray.docdemo.SimpleJob"
        runtime_env = dict(
            working_dir=working_dir,
            env_vars={"TEST_KEY": "TEST_VALUE"},
        )

        job_id = client.submit_job(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
        )

        wait_for_condition(
            _check_job_succeeded, client=client, job_id=job_id, timeout=120
        )

        logs = client.get_job_logs(job_id)
        print(f"================== logs ================== \n {logs}")
        assert "try to get TEST_KEY: TEST_VALUE" in logs
        assert "try to get TEST_KEY from normal task: TEST_VALUE" in logs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
