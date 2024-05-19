import pytest
import sys
import tempfile
import os

from ray_release.util import ERROR_LOG_PATTERNS
from ray_release.job_manager.anyscale_job_manager import AnyscaleJobManager


class FakeJobResult:
    def __init__(self, _id: str):
        self.id = _id


def test_get_ray_error_logs():
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "log01"), "w") as f:
            f.writelines(ERROR_LOG_PATTERNS[:1])
        with open(os.path.join(tmpdir, "log02"), "w") as f:
            f.writelines(ERROR_LOG_PATTERNS + ["haha"])
        with open(os.path.join(tmpdir, "job-driver-w00t"), "w") as f:
            f.writelines("w00t")
        (
            job_driver_log,
            ray_error_log,
        ) = AnyscaleJobManager._find_job_driver_and_ray_error_logs(tmpdir)
        assert ray_error_log == "".join(ERROR_LOG_PATTERNS + ["haha"])
        assert job_driver_log == "w00t"


def test_get_last_logs_long_running_job():
    """Test calling get_last_logs() on long-running jobs.

    When the job is running longer than 4 hours, get_last_logs() should skip
    downloading the logs and return None.
    """
    anyscale_job_manager = AnyscaleJobManager(cluster_manager=None)
    anyscale_job_manager._duration = 4 * 3_600 + 1
    anyscale_job_manager._last_job_result = FakeJobResult(_id="foo")
    assert anyscale_job_manager.get_last_logs() is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
