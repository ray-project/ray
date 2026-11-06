import sys

import pytest
from anyscale.job.models import JobState

from ray_release.anyscale_util import Anyscale
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.job_manager.anyscale_job_manager import AnyscaleJobManager
from ray_release.test import Test


class FakeJobStatus:
    def __init__(self, state: JobState):
        self.state = state


class FakeCreateJobResponse:
    class Result:
        id = "job_id"

    result = Result()


class FakeSDK(Anyscale):
    def __init__(self):
        super().__init__()
        self.created_job = None

    def project_name_by_id(self, project_id: str) -> str:
        return "fake_project_name"

    def create_job(self, job_request):
        self.created_job = job_request
        return FakeCreateJobResponse()


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
    anyscale_job_manager._job_id = "foo"
    anyscale_job_manager.save_last_job_status(FakeJobStatus(state=JobState.SUCCEEDED))
    assert anyscale_job_manager.get_last_logs() is None


def test_run_job_exports_cluster_compute_name():
    fake_test = Test(name="fake_test")
    fake_sdk = FakeSDK()
    cluster_manager = ClusterManager(
        test=fake_test, project_id="fake_project_id", sdk=fake_sdk
    )
    cluster_manager.cluster_env_name = "cluster_env"
    cluster_manager.cluster_env_build_id = "build_id"
    cluster_manager.cluster_compute_id = "compute_id"
    cluster_manager.cluster_compute_name = "compute_name"

    anyscale_job_manager = AnyscaleJobManager(cluster_manager=cluster_manager)
    anyscale_job_manager._run_job("echo hi", {"USER_ENV": "1"})

    env_vars = fake_sdk.created_job.config.runtime_env["env_vars"]
    assert env_vars["USER_ENV"] == "1"
    assert env_vars["ANYSCALE_JOB_CLUSTER_ENV_NAME"] == "cluster_env"
    assert env_vars["ANYSCALE_JOB_CLUSTER_COMPUTE_NAME"] == "compute_name"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
