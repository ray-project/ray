import sys

import pytest

from ray_release.job_manager.anyscale_job_manager import AnyscaleJobManager


class FakeJobResult:
    def __init__(self, _id: str):
        self.id = _id


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
