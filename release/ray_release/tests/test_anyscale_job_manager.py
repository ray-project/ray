import sys
from unittest.mock import patch

import pytest
from anyscale.job.models import JobState
from anyscale.sdk.anyscale_client.api.default_api import DefaultApi

from ray_release.anyscale_util import Anyscale
from ray_release.cluster_manager.cluster_manager import ClusterManager
from ray_release.exception import JobStartupFailed
from ray_release.job_manager.anyscale_job_manager import AnyscaleJobManager
from ray_release.test import Test
from ray_release.tests.utils import APIDict


class FakeJobStatus:
    def __init__(self, state: JobState):
        self.state = state


class FakeSDK(Anyscale):
    def project_name_by_id(self, project_id: str) -> str:
        return "fake_project_name"


def _make_job_manager(uses_new_sdk=False):
    fake_test = Test(
        name="fake_test",
        cluster={"byod": {}, "anyscale_sdk_2026": uses_new_sdk},
    )
    fake_sdk = FakeSDK()
    cm = ClusterManager(test=fake_test, project_id="fake_project_id", sdk=fake_sdk)
    cm.cluster_name = "test_cluster_123"
    cm.cluster_env_name = "test_env"
    cm.cluster_env_build_id = "anyscale/image/test:1"
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
def test_submit_job_new_sdk(mock_job):
    """New SDK path submits via anyscale.job.submit with JobConfig."""
    jm = _make_job_manager(uses_new_sdk=True)
    mock_job.submit.return_value = "job_new_123"

    jm._run_job("echo hello", {"FOO": "bar"})

    assert jm._job_id == "job_new_123"
    mock_job.submit.assert_called_once()
    config = mock_job.submit.call_args[0][0]
    assert config.name == "test_cluster_123"
    assert config.entrypoint == "echo hello"
    assert config.image_uri == "anyscale/image/test:1"
    assert config.compute_config == "cc_123"
    assert config.max_retries == 0
    assert "FOO" in config.env_vars


@patch.object(DefaultApi, "create_job")
def test_submit_job_legacy(mock_create_job):
    """Legacy path submits via DefaultApi.create_job."""
    jm = _make_job_manager(uses_new_sdk=False)
    jm.cluster_manager.cluster_env_build_id = "bld_legacy_123"
    mock_create_job.return_value = APIDict(result=APIDict(id="job_legacy_456"))

    jm._run_job("echo hello", {"FOO": "bar"}, upload_path="/tmp/upload")

    assert jm._job_id == "job_legacy_456"
    mock_create_job.assert_called_once()
    job_request = mock_create_job.call_args[0][1]
    assert job_request.name == "test_cluster_123"
    assert job_request.config.entrypoint == "echo hello"
    assert job_request.config.build_id == "bld_legacy_123"
    assert job_request.config.compute_config_id == "cc_123"
    assert "FOO" in job_request.config.runtime_env["env_vars"]


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_submit_job_new_sdk_failure_raises_startup_failed(mock_job):
    """New SDK path wraps exceptions in JobStartupFailed."""
    jm = _make_job_manager(uses_new_sdk=True)
    mock_job.submit.side_effect = RuntimeError("API error")

    with pytest.raises(JobStartupFailed):
        jm._run_job("echo hello", {})


@patch.object(DefaultApi, "create_job")
def test_submit_job_legacy_failure_raises_startup_failed(mock_create_job):
    """Legacy path wraps exceptions in JobStartupFailed."""
    jm = _make_job_manager(uses_new_sdk=False)
    mock_create_job.side_effect = RuntimeError("API error")

    with pytest.raises(JobStartupFailed):
        jm._run_job("echo hello", {})


@patch("ray_release.job_manager.anyscale_job_manager.anyscale.job")
def test_terminate_job_new_sdk(mock_job):
    """New SDK path terminates via anyscale.job.terminate."""
    jm = _make_job_manager(uses_new_sdk=True)
    jm._job_id = "job_123"
    jm._last_job_result = None  # No terminal state yet

    jm._terminate_job()

    mock_job.terminate.assert_called_once_with(id="job_123")


@patch.object(DefaultApi, "terminate_job")
def test_terminate_job_legacy(mock_terminate):
    """Legacy path terminates via DefaultApi.terminate_job."""
    jm = _make_job_manager(uses_new_sdk=False)
    jm._job_id = "job_456"
    jm._last_job_result = None

    jm._terminate_job()

    mock_terminate.assert_called_once()


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


def test_uses_new_sdk():
    """_uses_new_sdk() returns True when anyscale_sdk_2026 is set."""
    jm_new = _make_job_manager(uses_new_sdk=True)
    assert jm_new._uses_new_sdk() is True

    jm_legacy = _make_job_manager(uses_new_sdk=False)
    assert jm_legacy._uses_new_sdk() is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
