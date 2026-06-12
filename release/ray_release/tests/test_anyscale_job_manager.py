import sys
from unittest.mock import patch

import pytest
from anyscale.job.models import JobState

from ray_release.anyscale_util import Anyscale
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import JobStartupFailed
from ray_release.job_manager.anyscale_job_manager import AnyscaleJobManager
from ray_release.test import Test


class FakeJobStatus:
    def __init__(self, state: JobState):
        self.state = state


class FakeSDK(Anyscale):
    def project_name_by_id(self, project_id: str) -> str:
        return "fake_project_name"


class MockTest(Test):
    def get_anyscale_byod_image(self, build_id=None) -> str:
        return "anyscale/ray:nightly-py310-cpu"


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


def _make_job_manager() -> AnyscaleJobManager:
    fake_test = MockTest(name="fake_test")
    fake_sdk = FakeSDK()
    cluster_manager = ClusterManager(
        test=fake_test, project_id="fake_project_id", sdk=fake_sdk
    )
    cluster_manager.cluster_name = "cluster_name"
    cluster_manager.cluster_env_name = "cluster_env"
    cluster_manager.cluster_env_build_id = "build_id"
    cluster_manager.cluster_compute_id = "compute_id"
    cluster_manager.cluster_compute_name = "compute_name"
    return AnyscaleJobManager(cluster_manager=cluster_manager)


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_run_job_submits_job_config(mock_job):
    """_run_job submits a JobConfig via anyscale.job.submit()."""
    mock_job.submit.return_value = "job_123"
    job_manager = _make_job_manager()

    job_manager._run_job("echo hi", {"USER_ENV": "1"})

    assert job_manager._job_id == "job_123"
    mock_job.submit.assert_called_once()
    config = mock_job.submit.call_args[0][0]
    assert config.name == "cluster_name"
    assert config.entrypoint == "echo hi"
    assert config.image_uri == "anyscale/ray:nightly-py310-cpu"
    assert config.compute_config == "compute_name"
    assert config.max_retries == 0
    # Cluster env/compute names are exported to the job's environment.
    assert config.env_vars["USER_ENV"] == "1"
    assert config.env_vars["ANYSCALE_JOB_CLUSTER_ENV_NAME"] == "cluster_env"
    assert config.env_vars["ANYSCALE_JOB_CLUSTER_COMPUTE_NAME"] == "compute_name"


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_run_job_failure_raises_startup_failed(mock_job):
    """Submission errors are wrapped in JobStartupFailed."""
    mock_job.submit.side_effect = RuntimeError("API error")
    job_manager = _make_job_manager()

    with pytest.raises(JobStartupFailed):
        job_manager._run_job("echo hi", {})


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_terminate_job(mock_job):
    """_terminate_job calls anyscale.job.terminate()."""
    job_manager = _make_job_manager()
    job_manager._job_id = "job_123"
    job_manager._last_job_result = None

    job_manager._terminate_job()

    mock_job.terminate.assert_called_once_with(id="job_123")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
