from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from ray.dashboard.modules.job import job_manager as job_manager_module
from ray.dashboard.modules.job.common import JobInfo
from ray.dashboard.modules.job.job_manager import JobManager
from ray.job_submission import JobStatus


def _make_job_manager() -> JobManager:
    manager = object.__new__(JobManager)
    manager.JOB_MONITOR_LOOP_PERIOD_S = 0
    manager._job_info_client = MagicMock()
    manager._job_info_client.get_info = AsyncMock()
    manager._job_info_client.get_status = AsyncMock()
    manager._job_info_client.put_status = AsyncMock()
    manager._get_actor_for_job = MagicMock(return_value=None)
    manager._timeout_check_timer = MagicMock()
    manager._log_client = MagicMock()
    manager.event_logger = MagicMock()
    return manager


@pytest.mark.asyncio
@pytest.mark.parametrize("job_status", [None, JobStatus.SUCCEEDED, JobStatus.FAILED])
async def test_recovered_monitor_stops_for_deleted_or_terminal_job(job_status):
    manager = _make_job_manager()
    manager._job_info_client.get_status.return_value = job_status

    with patch.object(job_manager_module.ray, "kill") as kill:
        await manager._monitor_job_internal("job-id")

    manager._job_info_client.put_status.assert_not_awaited()
    manager._get_actor_for_job.assert_not_called()
    kill.assert_not_called()
    if job_status is None:
        manager.event_logger.info.assert_not_called()
    else:
        manager.event_logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_recovered_pending_monitor_stops_when_job_info_was_deleted():
    manager = _make_job_manager()
    manager._job_info_client.get_status.return_value = JobStatus.PENDING
    manager._job_info_client.get_info.return_value = None

    await manager._monitor_job_internal("job-id")

    manager._job_info_client.get_info.assert_awaited_once_with(
        "job-id", timeout=job_manager_module._PENDING_JOB_INFO_FETCH_TIMEOUT_S
    )
    manager._job_info_client.put_status.assert_not_awaited()
    manager._get_actor_for_job.assert_not_called()


@pytest.mark.asyncio
async def test_recovered_pending_monitor_retries_temporary_get_info_failure():
    manager = _make_job_manager()
    manager._job_info_client.get_status.side_effect = [
        JobStatus.PENDING,
        JobStatus.SUCCEEDED,
    ]
    manager._job_info_client.get_info.side_effect = RuntimeError(
        "GCS is temporarily unavailable"
    )

    await manager._monitor_job_internal("job-id")

    assert manager._job_info_client.get_status.await_args_list == [
        call("job-id", timeout=None),
        call("job-id", timeout=None),
    ]
    manager._job_info_client.get_info.assert_awaited_once_with(
        "job-id", timeout=job_manager_module._PENDING_JOB_INFO_FETCH_TIMEOUT_S
    )
    manager._job_info_client.put_status.assert_not_awaited()
    manager._get_actor_for_job.assert_not_called()
    manager.event_logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_recovered_pending_monitor_preserves_new_terminal_status():
    manager = _make_job_manager()
    manager._job_info_client.get_status.return_value = JobStatus.PENDING
    manager._job_info_client.get_info.return_value = JobInfo(
        entrypoint="echo hello", status=JobStatus.SUCCEEDED
    )

    await manager._monitor_job_internal("job-id")

    manager._job_info_client.put_status.assert_not_awaited()
    manager._get_actor_for_job.assert_not_called()
    manager.event_logger.info.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("latest_status", [None, JobStatus.SUCCEEDED])
async def test_recovered_monitor_exception_does_not_rewrite_changed_job(
    latest_status,
):
    manager = _make_job_manager()
    manager._job_info_client.get_status.side_effect = [
        JobStatus.RUNNING,
        latest_status,
    ]
    job_supervisor = MagicMock()
    job_supervisor.ping.options.return_value.remote.side_effect = RuntimeError(
        "supervisor exited"
    )

    with patch.object(job_manager_module.ray, "kill") as kill:
        await manager._monitor_job_internal("job-id", job_supervisor)

    manager._job_info_client.put_status.assert_not_awaited()
    kill.assert_not_called()
    if latest_status is None:
        manager.event_logger.info.assert_not_called()
    else:
        manager.event_logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_recovered_monitor_stops_when_guarded_status_update_loses_race():
    manager = _make_job_manager()
    manager._job_info_client.get_status.return_value = JobStatus.RUNNING
    manager._job_info_client.put_status.return_value = False

    with patch.object(job_manager_module.ray, "kill") as kill:
        await manager._monitor_job_internal("job-id")

    manager._job_info_client.put_status.assert_awaited_once()
    assert manager._job_info_client.put_status.await_args.kwargs["jobinfo_must_exist"]
    assert (
        manager._job_info_client.put_status.await_args.kwargs["expected_status"]
        == JobStatus.RUNNING
    )
    kill.assert_not_called()
