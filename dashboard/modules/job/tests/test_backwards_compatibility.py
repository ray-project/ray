import logging

import pytest
import sys
import os
import subprocess
import uuid
from contextlib import contextmanager

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
    # TODO (architkulkarni): Reenable test after #22368 is merged, and make the
    # it backwards compatibility script install the commit from #22368.
    @pytest.mark.skip("#22368 breaks backwards compatibility of the package REST API.")
    def test_cli(self):
        """
        1) Create a new conda environment with ray version X installed
            inherits same env as current conda envionment except ray version
        2) Start head node and dashboard with ray version X
        3) Use current commit's CLI code to do sample job submission flow
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
