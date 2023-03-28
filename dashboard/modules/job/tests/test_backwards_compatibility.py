import logging

import pytest
import sys
import os
import subprocess
import uuid
from contextlib import contextmanager

from ray.job_submission import JobSubmissionClient, JobStatus
from ray._private.test_utils import wait_for_condition

logger = logging.getLogger(__name__)


@contextmanager
def conda_env(env_name):
    # Set env name for shell script
    os.environ["JOB_COMPATIBILITY_TEST_TEMP_ENV"] = env_name
    # Delete conda env if it already exists
    try:
        yield
    finally:
        # Clean up created conda env upon test exit to prevent leaking
        del os.environ["JOB_COMPATIBILITY_TEST_TEMP_ENV"]
        subprocess.run(
            f"conda env remove -y --name {env_name}", shell=True, stdout=subprocess.PIPE
        )


def _compatibility_script_path(file_name: str) -> str:
    return os.path.join(
        os.path.dirname(__file__), "backwards_compatibility_scripts", file_name
    )


class TestBackwardsCompatibility:
    def test_cli(self):
        """
        Test that the current commit's CLI works with old server-side Ray versions.

        1) Create a new conda environment with old ray version X installed;
            inherits same env as current conda envionment except ray version
        2) (Server) Start head node and dashboard with old ray version X
        3) (Client) Use current commit's CLI code to do sample job submission flow
        4) Deactivate the new conda environment and back to original place
        """
        # Shell script creates and cleans up tmp conda environment regardless
        # of the outcome
        env_name = f"jobs-backwards-compatibility-{uuid.uuid4().hex}"
        with conda_env(env_name):
            shell_cmd = f"{_compatibility_script_path('test_backwards_compatibility.sh')}"  # noqa: E501

            try:
                subprocess.check_output(shell_cmd, shell=True, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as e:
                logger.error(str(e))
                logger.error(e.stdout.decode())
                raise e


@pytest.mark.skipif(
    os.environ.get("JOB_COMPATIBILITY_TEST_TEMP_ENV") is None,
    reason="This test is only meant to be run from the "
    "test_backwards_compatibility.sh shell script.",
)
def test_error_message():
    """
    Check that we get a good error message when running against an old server version.
    """
    client = JobSubmissionClient("http://127.0.0.1:8265")

    # Check that a basic job successfully runs.
    job_id = client.submit_job(
        entrypoint="echo 'hello world'",
    )
    wait_for_condition(lambda: client.get_job_status(job_id) == JobStatus.SUCCEEDED)

    # `entrypoint_num_cpus`, `entrypoint_num_gpus`, and `entrypoint_resources`
    # are not supported in ray<2.2.0.
    for unsupported_submit_kwargs in [
        {"entrypoint_num_cpus": 1},
        {"entrypoint_num_gpus": 1},
        {"entrypoint_resources": {"custom": 1}},
    ]:
        with pytest.raises(
            Exception,
            match="Ray version 2.0.1 is running on the cluster. "
            "`entrypoint_num_cpus`, `entrypoint_num_gpus`, and "
            "`entrypoint_resources` kwargs"
            " are not supported on the Ray cluster. Please ensure the cluster is "
            "running Ray 2.2 or higher.",
        ):
            client.submit_job(
                entrypoint="echo hello",
                **unsupported_submit_kwargs,
            )

    assert True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
