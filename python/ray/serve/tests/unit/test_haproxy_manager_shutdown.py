"""HAProxyManager shutdown ordering without Ray actors or subprocesses."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from ray.serve._private.haproxy import HAProxyManager


@pytest.mark.asyncio
async def test_shutdown_stops_subscription_and_awaits_coalesced_update():
    # Exercise the actual actor implementation's shutdown method. Construction
    # starts external services, so supply only its shutdown dependencies here.
    manager_class = HAProxyManager.__ray_metadata__.modified_class
    manager = manager_class.__new__(manager_class)
    manager._node_id = "shutdown-test-node"
    manager._update_pending = True
    events = []
    started = asyncio.Event()
    cancellation_observed = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def pending_update():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append("update-cancelled")
            cancellation_observed.set()
            # Cancellation itself is insufficient: shutdown must await this
            # cleanup before stopping the process the update could still use.
            await finish_cleanup.wait()
            events.append("update-finished")

    manager._coalesce_task = asyncio.create_task(pending_update())
    manager.long_poll_client = Mock(spec=["stop"])
    manager.long_poll_client.stop.side_effect = lambda: events.append(
        "subscription-stopped"
    )

    async def stop_process():
        assert manager._coalesce_task.done()
        assert manager._coalesce_task.cancelled()
        events.append("haproxy-stopped")

    manager._haproxy = Mock(spec=["stop"])
    manager._haproxy.stop = AsyncMock(side_effect=stop_process)
    metrics_collector = Mock(spec=["close"])
    metrics_collector.close.side_effect = lambda: events.append("metrics-closed")
    manager._metrics_collector = metrics_collector
    tasks = [manager._coalesce_task]

    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        shutdown_task = asyncio.create_task(manager.shutdown())
        cancellation_waiter = asyncio.create_task(cancellation_observed.wait())
        tasks.extend([shutdown_task, cancellation_waiter])
        done, _ = await asyncio.wait(
            [shutdown_task, cancellation_waiter],
            timeout=5,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if shutdown_task in done:
            # Surface a process-stop ordering assertion directly rather than
            # obscuring it behind a timeout waiting for missing cancellation.
            await shutdown_task
            pytest.fail("Shutdown finished before update cleanup was released")
        assert cancellation_waiter in done, "Pending update was not cancelled"
        manager.long_poll_client.stop.assert_called_once_with()
        assert manager._update_pending is False
        assert events == ["subscription-stopped", "update-cancelled"]
        assert not manager._coalesce_task.done()
        assert not shutdown_task.done()
        manager._haproxy.stop.assert_not_awaited()

        finish_cleanup.set()
        await asyncio.wait_for(shutdown_task, timeout=5)
        assert manager._coalesce_task.cancelled()
        manager._haproxy.stop.assert_awaited_once_with()
        metrics_collector.close.assert_called_once_with()
        assert manager._metrics_collector is None
        assert events == [
            "subscription-stopped",
            "update-cancelled",
            "update-finished",
            "haproxy-stopped",
            "metrics-closed",
        ]
    finally:
        finish_cleanup.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
