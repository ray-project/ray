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
async def test_recovered_pending_monitor_retries_when_timeout_update_loses_race():
    manager = _make_job_manager()
    manager._job_info_client.get_status.side_effect = [
        JobStatus.PENDING,
        JobStatus.RUNNING,
        JobStatus.SUCCEEDED,
    ]
    manager._job_info_client.get_info.return_value = JobInfo(
        entrypoint="echo hello", status=JobStatus.PENDING, start_time=0
    )
    manager._job_info_client.put_status.return_value = False
    manager._timeout_check_timer.time.return_value = 1000
    job_supervisor = MagicMock()
    ping_ref = MagicMock()
    job_supervisor.ping.options.return_value.remote.return_value = ping_ref

    with (
        patch.dict(
            job_manager_module.os.environ,
            {job_manager_module.RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR: "1"},
        ),
        patch.object(job_manager_module.ray, "wait", return_value=([], [])) as wait,
        patch.object(job_manager_module.ray, "kill") as kill,
    ):
        await manager._monitor_job_internal("job-id", job_supervisor)

    assert manager._job_info_client.get_status.await_args_list == [
        call("job-id", timeout=None),
        call("job-id", timeout=None),
        call("job-id", timeout=None),
    ]
    manager._job_info_client.put_status.assert_awaited_once()
    assert (
        manager._job_info_client.put_status.await_args.kwargs["expected_status"]
        == JobStatus.PENDING
    )
    job_supervisor.ping.options.return_value.remote.assert_called_once()
    wait.assert_called_once_with([ping_ref], timeout=0)
    kill.assert_not_called()


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
async def test_recovered_pending_monitor_retries_when_exception_update_loses_race():
    manager = _make_job_manager()
    manager._job_info_client.get_status.side_effect = [
        JobStatus.PENDING,
        JobStatus.PENDING,
        JobStatus.RUNNING,
        JobStatus.SUCCEEDED,
    ]
    manager._job_info_client.get_info.return_value = JobInfo(
        entrypoint="echo hello", status=JobStatus.PENDING, start_time=1000
    )
    manager._job_info_client.put_status.return_value = False
    manager._timeout_check_timer.time.return_value = 1
    job_supervisor = MagicMock()
    ping_ref = MagicMock()
    job_supervisor.ping.options.return_value.remote.side_effect = [
        RuntimeError("supervisor ping failed"),
        ping_ref,
    ]

    with (
        patch.dict(
            job_manager_module.os.environ,
            {job_manager_module.RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR: "1"},
        ),
        patch.object(job_manager_module.ray, "wait", return_value=([], [])) as wait,
        patch.object(job_manager_module.ray, "kill") as kill,
    ):
        await manager._monitor_job_internal("job-id", job_supervisor)

    assert manager._job_info_client.get_status.await_args_list == [
        call("job-id", timeout=None),
        call("job-id", timeout=None),
        call("job-id", timeout=None),
        call("job-id", timeout=None),
    ]
    manager._job_info_client.put_status.assert_awaited_once()
    assert (
        manager._job_info_client.put_status.await_args.kwargs["expected_status"]
        == JobStatus.PENDING
    )
    assert job_supervisor.ping.options.return_value.remote.call_count == 2
    wait.assert_called_once_with([ping_ref], timeout=0)
    kill.assert_not_called()


@pytest.mark.asyncio
async def test_recovered_monitor_stops_when_missing_supervisor_update_loses_race():
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
