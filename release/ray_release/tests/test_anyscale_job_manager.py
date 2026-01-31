import sys

import pytest

from ray_release.anyscale_util import Anyscale
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.job_manager.anyscale_job_manager import AnyscaleJobManager
from ray_release.test import Test


class FakeJobResult:
    def __init__(self, _id: str):
        self.id = _id


class FakeSDK(Anyscale):
    def project_name_by_id(self, project_id: str) -> str:
        return "fake_project_name"


def test_get_last_logs_long_running_job():
    """Test calling get_last_logs() on long-running jobs.

    When the job is running longer than 4 hours, get_last_logs() should skip
    downloading the logs and return None.
    """
    fake_test = Test(name="fake_test")
    fake_sdk = FakeSDK()
    cluster_manager = ClusterManager(
        test=fake_test, project_id="fake_project_id", sdk=fake_sdk
    )
    anyscale_job_manager = AnyscaleJobManager(cluster_manager=cluster_manager)
    anyscale_job_manager._duration = 4 * 3_600 + 1
    anyscale_job_manager.save_last_job_result(FakeJobResult(_id="foo"))
    assert anyscale_job_manager.get_last_logs() is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
