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


def _make_job_manager(uses_new_sdk=False):
    fake_test = Test(
        {
            "name": "fake_test",
            "cluster": {"byod": {}, "anyscale_sdk_2026": uses_new_sdk},
        }
    )
    fake_sdk = FakeSDK()
    cm = ClusterManager(test=fake_test, project_id="fake_project_id", sdk=fake_sdk)
    cm.cluster_name = "test_cluster_123"
    cm.cluster_env_name = "test_env"
    cm.cluster_env_build_id = "anyscale/image/test:1"
    cm.cluster_compute_name = "test_compute_config"
    cm.cluster_compute_id = "cc_123"
    cm.smoke_test = False
    return AnyscaleJobManager(cluster_manager=cm)


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


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_submit_job(mock_job):
    """_run_job submits via anyscale.job.submit with correct JobConfig fields."""
    jm = _make_job_manager()
    mock_job.submit.return_value = "job_123"

    jm._run_job("echo hello", {"FOO": "bar"})

    assert jm._job_id == "job_123"
    mock_job.submit.assert_called_once()
    config = mock_job.submit.call_args[0][0]
    assert config.name == "test_cluster_123"
    assert config.entrypoint == "echo hello"
    assert config.image_uri == "anyscale/image/test:1"
    assert config.compute_config == "test_compute_config"
    assert config.max_retries == 0
    assert "FOO" in config.env_vars


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_submit_job_failure_raises_startup_failed(mock_job):
    """Job submission wraps exceptions in JobStartupFailed."""
    jm = _make_job_manager(uses_new_sdk=True)
    mock_job.submit.side_effect = RuntimeError("API error")

    with pytest.raises(JobStartupFailed):
        jm._run_job("echo hello", {})


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_terminate_job(mock_job):
    """Terminate calls anyscale.job.terminate."""
    jm = _make_job_manager(uses_new_sdk=False)
    jm._job_id = "job_123"
    jm._last_job_result = None

    jm._terminate_job()

    mock_job.terminate.assert_called_once_with(id="job_123")


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_terminate_job_skips_when_no_job(mock_job):
    """Terminate is a no-op when no job ID exists."""
    jm = _make_job_manager(uses_new_sdk=True)
    jm._job_id = None
    jm._terminate_job()
    mock_job.terminate.assert_not_called()


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_terminate_job_skips_when_terminal(mock_job):
    """Terminate is a no-op when job is already in terminal state."""
    jm = _make_job_manager(uses_new_sdk=True)
    jm._job_id = "job_123"
    jm.save_last_job_status(FakeJobStatus(state=JobState.SUCCEEDED))
    jm._terminate_job()
    mock_job.terminate.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
