import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

import ray.dashboard.optional_utils as optional_utils
from ray.dashboard.modules.job import (
    job_agent as job_agent_module,
    job_manager as job_manager_module,
)
from ray.dashboard.modules.job.job_agent import JobAgent
from ray.dashboard.modules.job.job_manager import JobManager
from ray.job_submission import JobStatus


def _make_job_agent(*, is_head: bool) -> JobAgent:
    dashboard_agent = MagicMock()
    dashboard_agent.is_head = is_head
    dashboard_agent.gcs_address = "127.0.0.1:6379"
    dashboard_agent.gcs_client = MagicMock()
    dashboard_agent.log_dir = "/tmp/ray/session_latest/logs"
    return JobAgent(dashboard_agent)


@pytest.mark.asyncio
async def test_head_job_agent_eagerly_initializes_job_manager():
    agent = _make_job_agent(is_head=True)
    job_manager = MagicMock()

    with (
        patch.object(optional_utils, "init_ray_connection") as init_ray_connection,
        patch.object(job_agent_module, "JobManager", return_value=job_manager),
    ):
        await agent.run(server=None)

    init_ray_connection.assert_called_once_with("127.0.0.1:6379")
    assert agent._job_manager is job_manager


@pytest.mark.asyncio
async def test_worker_job_agent_does_not_initialize_job_manager():
    agent = _make_job_agent(is_head=False)

    with (
        patch.object(optional_utils, "init_ray_connection") as init_ray_connection,
        patch.object(job_agent_module, "JobManager") as job_manager_cls,
    ):
        await agent.run(server=None)

    init_ray_connection.assert_not_called()
    job_manager_cls.assert_not_called()
    assert agent._job_manager is None


@pytest.mark.asyncio
async def test_job_agent_retries_ray_connection_failure():
    agent = _make_job_agent(is_head=True)
    job_manager = MagicMock()

    with (
        patch.object(
            optional_utils,
            "init_ray_connection",
            side_effect=[ConnectionError("GCS is unavailable"), None],
        ) as init_ray_connection,
        patch.object(job_agent_module, "JobManager", return_value=job_manager),
        patch.object(job_agent_module.random, "uniform", return_value=0),
        patch.object(
            job_agent_module.asyncio, "sleep", new_callable=AsyncMock
        ) as sleep,
    ):
        await agent.run(server=None)

    assert init_ray_connection.call_count == 2
    sleep.assert_awaited_once_with(30)
    assert agent._job_manager is job_manager


@pytest.mark.asyncio
async def test_job_agent_retries_job_manager_construction_failure():
    agent = _make_job_agent(is_head=True)
    job_manager = MagicMock()

    with (
        patch.object(optional_utils, "init_ray_connection") as init_ray_connection,
        patch.object(
            job_agent_module,
            "JobManager",
            side_effect=[RuntimeError("constructor failed"), job_manager],
        ) as job_manager_cls,
        patch.object(job_agent_module.random, "uniform", return_value=0),
        patch.object(
            job_agent_module.asyncio, "sleep", new_callable=AsyncMock
        ) as sleep,
    ):
        await agent.run(server=None)

    assert init_ray_connection.call_count == 2
    assert job_manager_cls.call_count == 2
    sleep.assert_awaited_once_with(30)
    assert agent._job_manager is job_manager


@pytest.mark.asyncio
async def test_job_agent_retry_delay_is_capped():
    agent = _make_job_agent(is_head=True)
    failures = [ConnectionError("GCS is unavailable")] * 6

    with (
        patch.object(
            optional_utils,
            "init_ray_connection",
            side_effect=[*failures, None],
        ),
        patch.object(job_agent_module, "JobManager", return_value=MagicMock()),
        patch.object(job_agent_module.random, "uniform", return_value=0),
        patch.object(
            job_agent_module.asyncio, "sleep", new_callable=AsyncMock
        ) as sleep,
    ):
        await agent.run(server=None)

    assert sleep.await_args_list == [
        call(30),
        call(60),
        call(120),
        call(240),
        call(300),
        call(300),
    ]


@pytest.mark.asyncio
async def test_job_agent_ray_init_does_not_block_event_loop():
    agent = _make_job_agent(is_head=True)

    def slow_init(_):
        time.sleep(0.3)

    with (
        patch.object(optional_utils, "init_ray_connection", side_effect=slow_init),
        patch.object(job_agent_module, "JobManager", return_value=MagicMock()),
    ):
        loop = asyncio.get_running_loop()
        start = loop.time()
        run_task = asyncio.create_task(agent.run(server=None))
        await asyncio.sleep(0.01)
        elapsed = loop.time() - start
        await run_task

    assert elapsed < 0.2


def test_init_ray_connection_is_idempotent():
    with (
        patch.object(optional_utils.ray, "is_initialized", return_value=True),
        patch.object(optional_utils.ray, "init") as ray_init,
    ):
        optional_utils.init_ray_connection("127.0.0.1:6379")

    ray_init.assert_not_called()


def test_init_ray_connection_uses_dashboard_settings(monkeypatch):
    monkeypatch.delenv("RAY_gcs_server_request_timeout_seconds", raising=False)

    with (
        patch.object(optional_utils.ray, "is_initialized", return_value=False),
        patch.object(optional_utils.ray, "init") as ray_init,
    ):
        optional_utils.init_ray_connection("127.0.0.1:6379")

    assert optional_utils.os.environ["RAY_gcs_server_request_timeout_seconds"] == str(
        optional_utils.dashboard_consts.GCS_RPC_TIMEOUT_SECONDS
    )
    ray_init.assert_called_once_with(
        address="127.0.0.1:6379",
        log_to_driver=False,
        configure_logging=False,
        namespace=optional_utils.RAY_INTERNAL_DASHBOARD_NAMESPACE,
        _skip_env_hook=True,
    )


def test_init_ray_connection_preserves_init_error_when_shutdown_fails():
    with (
        patch.object(optional_utils.ray, "is_initialized", return_value=False),
        patch.object(
            optional_utils.ray,
            "init",
            side_effect=ConnectionError("GCS is unavailable"),
        ),
        patch.object(
            optional_utils.ray,
            "shutdown",
            side_effect=RuntimeError("shutdown failed"),
        ) as ray_shutdown,
    ):
        with pytest.raises(ConnectionError, match="GCS is unavailable"):
            optional_utils.init_ray_connection("127.0.0.1:6379")

    ray_shutdown.assert_called_once_with()


@pytest.mark.asyncio
async def test_recovery_scan_retries_then_monitors_non_terminal_jobs():
    pending_job = MagicMock(status=JobStatus.PENDING)
    finished_job = MagicMock(status=JobStatus.SUCCEEDED)
    manager = MagicMock()
    manager._job_info_client.get_all_jobs = AsyncMock(
        side_effect=[
            RuntimeError("GCS is unavailable"),
            {"pending": pending_job, "finished": finished_job},
        ]
    )
    manager._recover_running_jobs_event = asyncio.Event()
    manager._monitor_job = MagicMock()

    with (
        patch.object(
            job_manager_module.asyncio, "sleep", new_callable=AsyncMock
        ) as sleep,
        patch.object(job_manager_module, "run_background_task") as run_task,
    ):
        await JobManager._recover_running_jobs(manager)

    assert manager._job_info_client.get_all_jobs.await_args_list == [
        call(timeout=5),
        call(timeout=5),
    ]
    sleep.assert_awaited_once_with(1)
    manager._monitor_job.assert_called_once_with("pending")
    run_task.assert_called_once_with(manager._monitor_job.return_value)
    assert manager._recover_running_jobs_event.is_set()


@pytest.mark.asyncio
async def test_recovery_scan_exhaustion_still_unblocks_submissions():
    manager = MagicMock()
    manager._job_info_client.get_all_jobs = AsyncMock(
        side_effect=RuntimeError("GCS is unavailable")
    )
    manager._recover_running_jobs_event = asyncio.Event()
    manager._monitor_job = MagicMock()

    with patch.object(
        job_manager_module.asyncio, "sleep", new_callable=AsyncMock
    ) as sleep:
        await JobManager._recover_running_jobs(manager)

    assert manager._job_info_client.get_all_jobs.await_count == 5
    assert sleep.await_args_list == [call(1), call(2), call(4), call(8)]
    manager._monitor_job.assert_not_called()
    assert manager._recover_running_jobs_event.is_set()


@pytest.mark.asyncio
async def test_recovery_scan_enforces_total_deadline():
    manager = MagicMock()
    release_scan = asyncio.Event()
    scan_tasks = []

    async def get_all_jobs(*, timeout):
        scan_tasks.append(asyncio.current_task())
        try:
            await release_scan.wait()
        except asyncio.CancelledError:
            # Simulate a GCS awaitable that does not finish cancellation promptly.
            await release_scan.wait()

    manager._job_info_client.get_all_jobs = AsyncMock(side_effect=get_all_jobs)
    manager._recover_running_jobs_event = asyncio.Event()

    with patch.object(job_manager_module, "_RECOVERY_SCAN_TOTAL_BUDGET_S", 0.01):
        await asyncio.wait_for(JobManager._recover_running_jobs(manager), timeout=1)

    manager._job_info_client.get_all_jobs.assert_awaited_once_with(timeout=5)
    assert manager._recover_running_jobs_event.is_set()
    release_scan.set()
    await asyncio.gather(*scan_tasks)


@pytest.mark.asyncio
async def test_submit_job_fails_instead_of_waiting_forever_for_recovery():
    manager = MagicMock()
    manager._recover_running_jobs_event = asyncio.Event()

    with patch.object(job_manager_module, "_RECOVERY_SUBMISSION_WAIT_TIMEOUT_S", 0):
        with pytest.raises(RuntimeError, match="recovery did not complete"):
            await JobManager.submit_job(
                manager,
                entrypoint="echo hello",
                submission_id="submission-id",
            )
