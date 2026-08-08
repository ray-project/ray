"""Tests for eager JobManager initialization on head node startup.

Tests verify that:
1. Head node JobAgent eagerly initializes JobManager in run() without HTTP requests.
2. Worker node JobAgent does NOT initialize JobManager in run().
3. Initialization retries with exponential backoff on failure.
4. GCS responsiveness is verified before JobManager initialization.
5. ray.shutdown() is only called if ray was initialized by _initialize_job_manager.

These tests use mocking to avoid requiring a running Ray cluster or compiled
C extensions, making them fast and self-contained unit tests.
"""

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_mock_dashboard_agent(is_head=True):
    """Create a mock DashboardAgent with the required attributes."""
    mock_agent = MagicMock()
    mock_agent.is_head = is_head
    mock_agent.gcs_address = "127.0.0.1:6379"
    mock_agent.gcs_client = MagicMock()
    mock_agent.log_dir = "/tmp/ray/logs"
    mock_agent.session_name = "test_session"
    return mock_agent


@pytest.mark.asyncio
async def test_head_node_initializes_job_manager():
    """On the head node, run() should trigger JobManager creation."""
    from ray.dashboard.modules.job.job_agent import JobAgent

    mock_agent = _make_mock_dashboard_agent(is_head=True)
    job_agent = JobAgent(mock_agent)
    assert job_agent._job_manager is None

    with patch("ray.dashboard.modules.job.job_agent.run_background_task") as mock_bg:
        await job_agent.run(server=None)
        # run() should have scheduled _initialize_job_manager as a background task
        mock_bg.assert_called_once()
        # The argument should be a coroutine from _initialize_job_manager
        coro_arg = mock_bg.call_args[0][0]
        assert asyncio.iscoroutine(coro_arg)
        # Clean up the unawaited coroutine to avoid RuntimeWarning
        coro_arg.close()


@pytest.mark.asyncio
async def test_worker_node_does_not_initialize_job_manager():
    """On a worker node, run() should NOT create JobManager."""
    from ray.dashboard.modules.job.job_agent import JobAgent

    mock_agent = _make_mock_dashboard_agent(is_head=False)
    job_agent = JobAgent(mock_agent)
    assert job_agent._job_manager is None

    with patch("ray.dashboard.modules.job.job_agent.run_background_task") as mock_bg:
        await job_agent.run(server=None)
        # run() should return immediately without scheduling any background task
        mock_bg.assert_not_called()
    # _job_manager should remain None
    assert job_agent._job_manager is None


@pytest.mark.asyncio
async def test_initialize_job_manager_retries_on_failure():
    """_initialize_job_manager should retry with backoff when ray.init fails."""
    from ray.dashboard.modules.job.job_agent import JobAgent

    mock_agent = _make_mock_dashboard_agent(is_head=True)
    job_agent = JobAgent(mock_agent)

    call_count = 0

    def mock_ray_init(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("GCS not ready")
        # On the 3rd call, succeed (do nothing)

    with (
        patch(
            "ray.dashboard.modules.job.job_agent.ray.is_initialized",
            return_value=False,
        ),
        patch(
            "ray.dashboard.modules.job.job_agent.ray.init",
            side_effect=mock_ray_init,
        ),
        patch("ray.dashboard.modules.job.job_agent.ray.shutdown"),
        patch(
            "ray.dashboard.modules.job.job_agent.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "ray.dashboard.modules.job.job_agent.dashboard_utils.get_head_node_id",
            new_callable=AsyncMock,
        ),
        patch.object(job_agent, "get_job_manager") as mock_get_jm,
    ):
        await job_agent._initialize_job_manager()

        # ray.init should have been called 3 times (2 failures + 1 success)
        assert call_count == 3
        # get_job_manager should be called once on success
        mock_get_jm.assert_called_once()
        # asyncio.sleep should have been called twice (for the 2 retries)
        assert mock_sleep.call_count == 2
        # Backoff should be 1s, then 2s
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)


@pytest.mark.asyncio
async def test_initialize_job_manager_succeeds_immediately():
    """_initialize_job_manager should succeed without retries when everything works."""
    from ray.dashboard.modules.job.job_agent import JobAgent

    mock_agent = _make_mock_dashboard_agent(is_head=True)
    job_agent = JobAgent(mock_agent)

    with (
        patch(
            "ray.dashboard.modules.job.job_agent.ray.is_initialized",
            return_value=False,
        ),
        patch("ray.dashboard.modules.job.job_agent.ray.init"),
        patch(
            "ray.dashboard.modules.job.job_agent.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "ray.dashboard.modules.job.job_agent.dashboard_utils.get_head_node_id",
            new_callable=AsyncMock,
        ),
        patch.object(job_agent, "get_job_manager") as mock_get_jm,
    ):
        await job_agent._initialize_job_manager()

        mock_get_jm.assert_called_once()
        # No retries needed, so sleep should never be called
        mock_sleep.assert_not_called()


@pytest.mark.asyncio
async def test_initialize_job_manager_skips_ray_init_if_already_initialized():
    """If ray is already initialized, _initialize_job_manager should skip ray.init."""
    from ray.dashboard.modules.job.job_agent import JobAgent

    mock_agent = _make_mock_dashboard_agent(is_head=True)
    job_agent = JobAgent(mock_agent)

    with (
        patch(
            "ray.dashboard.modules.job.job_agent.ray.is_initialized",
            return_value=True,
        ),
        patch("ray.dashboard.modules.job.job_agent.ray.init") as mock_init,
        patch(
            "ray.dashboard.modules.job.job_agent.dashboard_utils.get_head_node_id",
            new_callable=AsyncMock,
        ),
        patch.object(job_agent, "get_job_manager") as mock_get_jm,
    ):
        await job_agent._initialize_job_manager()

        # ray.init should NOT be called since ray is already initialized
        mock_init.assert_not_called()
        mock_get_jm.assert_called_once()


@pytest.mark.asyncio
async def test_initialize_job_manager_backoff_caps_at_max():
    """Backoff should cap at max_backoff_s (60s)."""
    from ray.dashboard.modules.job.job_agent import JobAgent

    mock_agent = _make_mock_dashboard_agent(is_head=True)
    job_agent = JobAgent(mock_agent)

    call_count = 0
    # Need enough failures to reach the cap: 1, 2, 4, 8, 16, 32, 64->60, 60
    num_failures = 8

    def mock_ray_init(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= num_failures:
            raise ConnectionError("GCS not ready")

    with (
        patch(
            "ray.dashboard.modules.job.job_agent.ray.is_initialized",
            return_value=False,
        ),
        patch(
            "ray.dashboard.modules.job.job_agent.ray.init",
            side_effect=mock_ray_init,
        ),
        patch("ray.dashboard.modules.job.job_agent.ray.shutdown"),
        patch(
            "ray.dashboard.modules.job.job_agent.asyncio.sleep",
            new_callable=AsyncMock,
        ) as mock_sleep,
        patch(
            "ray.dashboard.modules.job.job_agent.dashboard_utils.get_head_node_id",
            new_callable=AsyncMock,
        ),
        patch.object(job_agent, "get_job_manager"),
    ):
        await job_agent._initialize_job_manager()

        assert call_count == num_failures + 1
        # Verify backoff sequence: 1, 2, 4, 8, 16, 32, 60, 60
        sleep_values = [call.args[0] for call in mock_sleep.call_args_list]
        expected = [1, 2, 4, 8, 16, 32, 60, 60]
        assert sleep_values == expected


@pytest.mark.asyncio
async def test_initialize_job_manager_does_not_shutdown_preexisting_ray():
    """If ray was already initialized before _initialize_job_manager, failure should not call ray.shutdown."""
    from ray.dashboard.modules.job.job_agent import JobAgent

    mock_agent = _make_mock_dashboard_agent(is_head=True)
    job_agent = JobAgent(mock_agent)

    call_count = 0

    async def mock_get_head_node_id(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise TimeoutError("GCS unresponsive")

    with (
        patch(
            "ray.dashboard.modules.job.job_agent.ray.is_initialized",
            return_value=True,
        ),
        patch("ray.dashboard.modules.job.job_agent.ray.init") as mock_init,
        patch("ray.dashboard.modules.job.job_agent.ray.shutdown") as mock_shutdown,
        patch(
            "ray.dashboard.modules.job.job_agent.asyncio.sleep",
            new_callable=AsyncMock,
        ),
        patch(
            "ray.dashboard.modules.job.job_agent.dashboard_utils.get_head_node_id",
            side_effect=mock_get_head_node_id,
        ),
        patch.object(job_agent, "get_job_manager") as mock_get_jm,
    ):
        await job_agent._initialize_job_manager()

        # ray.init should not be called
        mock_init.assert_not_called()
        # ray.shutdown MUST NOT be called because ray was not initialized here
        mock_shutdown.assert_not_called()
        # get_job_manager called on the 2nd attempt after retry
        mock_get_jm.assert_called_once()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
