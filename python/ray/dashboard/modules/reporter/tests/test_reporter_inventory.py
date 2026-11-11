"""Unit tests for the reporter agent's accelerator inventory RPC.

Focuses on the vendor-neutral aggregation precedence, the typed reply
serialization, the node-lifetime cache, and the coalescing/cancellation
behavior of concurrent first requests. These use a bare ReporterAgent instance
(object.__new__) and mock discovery; they are not real GPU verification.
"""

import asyncio
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from ray._private.accelerators.accelerator import (
    AcceleratorDevice,
    AcceleratorInventory,
    AcceleratorInventoryStatus,
)
from ray.core.generated import reporter_pb2
from ray.dashboard.modules.reporter.reporter_agent import (
    ReporterAgent,
    _accelerator_inventory_to_reply,
    _discover_accelerator_inventory,
)


def _make_agent():
    agent = object.__new__(ReporterAgent)
    agent._accelerator_inventory_cache = None
    agent._accelerator_inventory_task = None
    agent._executor = ThreadPoolExecutor(max_workers=1)
    return agent


class _FakeManager:
    def __init__(self, inventory):
        self._inventory = inventory

    def get_current_node_accelerator_inventory(self):
        return self._inventory


def _ok(devices=None):
    return AcceleratorInventory(
        status=AcceleratorInventoryStatus.OK, devices=devices or []
    )


def _err(msg="boom"):
    return AcceleratorInventory(
        status=AcceleratorInventoryStatus.ERROR, status_message=msg
    )


def _patch_managers(managers):
    return patch(
        "ray._private.accelerators.get_all_accelerator_managers",
        return_value=managers,
    )


def test_discover_unsupported_when_no_implementation():
    with _patch_managers([_FakeManager(None), _FakeManager(None)]):
        result = _discover_accelerator_inventory()
    assert result.status is AcceleratorInventoryStatus.UNSUPPORTED


def test_discover_merges_ok_devices():
    d1 = AcceleratorDevice("nvidia", "GPU", "A100", "GPU-1", 10)
    d2 = AcceleratorDevice("nvidia", "GPU", "V100", "GPU-2", 20)
    with _patch_managers([_FakeManager(_ok([d1])), _FakeManager(_ok([d2]))]):
        result = _discover_accelerator_inventory()
    assert result.status is AcceleratorInventoryStatus.OK
    assert [d.uuid for d in result.devices] == ["GPU-1", "GPU-2"]


def test_discover_error_beats_ok():
    """A supported provider that failed must not be hidden behind another
    provider's successful inventory."""
    with _patch_managers([_FakeManager(_ok()), _FakeManager(_err())]):
        result = _discover_accelerator_inventory()
    assert result.status is AcceleratorInventoryStatus.ERROR


def test_discover_unavailable_when_no_ok():
    unavailable = AcceleratorInventory(
        status=AcceleratorInventoryStatus.UNAVAILABLE, status_message="no driver"
    )
    with _patch_managers([_FakeManager(None), _FakeManager(unavailable)]):
        result = _discover_accelerator_inventory()
    assert result.status is AcceleratorInventoryStatus.UNAVAILABLE


def test_reply_serialization():
    inventory = _ok(
        [
            AcceleratorDevice(
                vendor="nvidia",
                resource_family="GPU",
                model="A100",
                uuid="GPU-1",
                memory_total_bytes=1234,
                pci_bus_id="0000:3b:00.0",
                numa_node=1,
                kind="gpu",
                parent_uuid=None,
            )
        ]
    )
    reply = _accelerator_inventory_to_reply(inventory)
    assert reply.status == reporter_pb2.ACCELERATOR_INVENTORY_STATUS_OK
    assert len(reply.devices) == 1
    device = reply.devices[0]
    assert device.uuid == "GPU-1"
    assert device.memory_total_bytes == 1234
    assert device.pci_bus_id == "0000:3b:00.0"
    assert device.numa_node == 1
    assert device.HasField("numa_node")
    assert device.kind == "gpu"
    assert device.parent_uuid == ""


def test_reply_serialization_null_numa_and_pci():
    inventory = _ok(
        [
            AcceleratorDevice(
                "nvidia", "GPU", "A100", "GPU-1", 1234, pci_bus_id=None, numa_node=None
            )
        ]
    )
    device = _accelerator_inventory_to_reply(inventory).devices[0]
    assert not device.HasField("numa_node")
    assert device.pci_bus_id == ""


def test_ok_discovery_is_cached():
    agent = _make_agent()
    with patch(
        "ray.dashboard.modules.reporter.reporter_agent._discover_accelerator_inventory",
        side_effect=[_ok(), _ok()],
    ) as mock_discover:
        first = asyncio.run(agent._get_accelerator_inventory())
        second = asyncio.run(agent._get_accelerator_inventory())
    assert first.status is AcceleratorInventoryStatus.OK
    assert second.status is AcceleratorInventoryStatus.OK
    # Second call is served from cache; discovery ran once.
    assert mock_discover.call_count == 1


def test_non_ok_discovery_is_not_cached():
    agent = _make_agent()
    with patch(
        "ray.dashboard.modules.reporter.reporter_agent._discover_accelerator_inventory",
        side_effect=[
            AcceleratorInventory(
                status=AcceleratorInventoryStatus.UNAVAILABLE, status_message="x"
            ),
            _ok(),
        ],
    ) as mock_discover:
        first = asyncio.run(agent._get_accelerator_inventory())
        assert first.status is AcceleratorInventoryStatus.UNAVAILABLE
        assert agent._accelerator_inventory_cache is None
        second = asyncio.run(agent._get_accelerator_inventory())
    assert second.status is AcceleratorInventoryStatus.OK
    assert mock_discover.call_count == 2


def test_discovery_exception_surfaces_error():
    agent = _make_agent()
    with patch(
        "ray.dashboard.modules.reporter.reporter_agent._discover_accelerator_inventory",
        side_effect=RuntimeError("kaboom"),
    ):
        result = asyncio.run(agent._get_accelerator_inventory())
    assert result.status is AcceleratorInventoryStatus.ERROR
    assert "kaboom" in result.status_message


async def test_concurrent_requests_coalesce_and_survive_cancellation():
    agent = _make_agent()
    started = threading.Event()
    release = threading.Event()

    def slow_discovery():
        started.set()
        release.wait(timeout=5)
        return _ok()

    loop = asyncio.get_running_loop()
    with patch(
        "ray.dashboard.modules.reporter.reporter_agent._discover_accelerator_inventory",
        side_effect=slow_discovery,
    ):
        waiter1 = asyncio.ensure_future(agent._get_accelerator_inventory())
        waiter2 = asyncio.ensure_future(agent._get_accelerator_inventory())

        # Wait until the single shared discovery has started.
        await loop.run_in_executor(None, started.wait, 5)

        # Cancelling one waiter must not cancel the shared discovery.
        waiter1.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter1

        release.set()
        result = await waiter2

    assert result.status is AcceleratorInventoryStatus.OK
    # The successful result is cached for subsequent queries.
    assert agent._accelerator_inventory_cache is not None
    assert (await agent._get_accelerator_inventory()).status is (
        AcceleratorInventoryStatus.OK
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
