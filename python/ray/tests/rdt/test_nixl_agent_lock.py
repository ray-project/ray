"""Unit tests for cleaning up NIXL transfers.

They cover removing NIXL remote agents while transfers on them are in flight, and
returning receive pool blocks once a transfer is cleaned up. These use a fake NIXL
agent, so they run without NIXL or GPUs.
"""

import sys
import threading
import time

import pytest
import torch

from ray.experimental.rdt import nixl_memory_pool, nixl_tensor_transport
from ray.experimental.rdt.nixl_memory_pool import MemoryPoolManager
from ray.experimental.rdt.nixl_tensor_transport import NixlTensorTransport


class FakeAgent:
    """Records removals and flags one that happens while a transfer is in progress."""

    def __init__(self):
        # handle -> (remote name, polls left until the transfer is done)
        self.xfers = {}
        self.removed = []
        self.released = []
        self.removed_in_flight = []

    def check_xfer_state(self, handle):
        name, polls_left = self.xfers[handle]
        if polls_left == 0:
            return "DONE"
        self.xfers[handle] = (name, polls_left - 1)
        return "PROC"

    def add_remote_agent(self, meta):
        pass

    def remove_remote_agent(self, name):
        self.removed.append(name)
        if any(n == name and polls > 0 for n, polls in self.xfers.values()):
            self.removed_in_flight.append(name)

    def release_xfer_handle(self, handle):
        self.released.append(handle)


def start_transfer(transport, name, handle, polls=3):
    transport._nixl_agent.xfers[handle] = (name, polls)
    transport._inflight_xfers.setdefault(name, set()).add(handle)


@pytest.fixture
def transport(monkeypatch):
    monkeypatch.setattr(nixl_tensor_transport, "NIXL_REMOTE_AGENT_CACHE_MAXSIZE", 2)
    transport = NixlTensorTransport()
    transport._nixl_agent = FakeAgent()
    return transport


def test_version_change_waits_for_in_flight_transfer(transport):
    agent = transport._nixl_agent
    transport._prepare_remote_agent("a", 1, b"meta")
    start_transfer(transport, "a", "h1")

    transport._prepare_remote_agent("a", 2, b"meta")

    assert agent.removed == ["a"]
    assert agent.removed_in_flight == []


def test_same_version_keeps_agent(transport):
    agent = transport._nixl_agent
    transport._prepare_remote_agent("a", 1, b"meta")
    start_transfer(transport, "a", "h1")

    transport._prepare_remote_agent("a", 1, b"meta")

    assert agent.removed == []


def test_eviction_waits_for_in_flight_transfer(transport):
    agent = transport._nixl_agent
    transport._prepare_remote_agent("a", 1, b"meta")
    transport._prepare_remote_agent("b", 1, b"meta")
    start_transfer(transport, "a", "h1")

    transport._evict_if_full("c")

    assert agent.removed == ["a"]
    assert agent.removed_in_flight == []
    assert list(transport._remote_agents) == ["b"]


def test_concurrent_evictions_remove_an_agent_once(transport):
    agent = transport._nixl_agent
    transport._prepare_remote_agent("a", 1, b"meta")
    transport._prepare_remote_agent("b", 1, b"meta")

    with transport._remote_agent_lock("a"):
        threads = [
            threading.Thread(target=transport._evict_if_full, args=(name,))
            for name in ("c", "d")
        ]
        for t in threads:
            t.start()
        # Both threads pick "a" as the victim and wait for its lock.
        time.sleep(0.2)
    for t in threads:
        t.join()

    assert agent.removed == ["a"]


def test_without_cache_the_last_transfer_removes_the_agent(monkeypatch, transport):
    monkeypatch.setattr(nixl_tensor_transport, "NIXL_REMOTE_AGENT_CACHE_MAXSIZE", 0)
    agent = transport._nixl_agent
    start_transfer(transport, "a", "h1")
    start_transfer(transport, "a", "h2")

    transport._cleanup_transfer("obj1", [], "h1", "a", False)
    assert agent.removed == []

    transport._cleanup_transfer("obj2", [], "h2", "a", False)
    assert agent.released == ["h1", "h2"]
    assert agent.removed == ["a"]


@pytest.fixture
def pool(transport):
    transport._memory_pool = MemoryPoolManager(pool_size=64, device=torch.device("cpu"))
    return transport._memory_pool


def free_bytes(pool):
    return sum(block.size for block in pool._free_blocks)


def test_cleanup_frees_pool_blocks_after_releasing_the_handle(transport, pool):
    agent = transport._nixl_agent
    _, blocks = pool.allocate_regions([32])
    start_transfer(transport, "a", "h1")
    free_at_release = []
    release = agent.release_xfer_handle

    def record_release(handle):
        free_at_release.append(free_bytes(pool))
        release(handle)

    agent.release_xfer_handle = record_release

    transport._cleanup_transfer("obj", [], "h1", "a", False, blocks)

    assert agent.released == ["h1"]
    # The blocks were still allocated while the handle was released.
    assert free_at_release == [32]
    assert free_bytes(pool) == 64


def test_cleanup_frees_pool_blocks_if_releasing_the_handle_fails(transport, pool):
    agent = transport._nixl_agent
    _, blocks = pool.allocate_regions([32])
    start_transfer(transport, "a", "h1")

    def fail_release(handle):
        raise RuntimeError("NIXL_ERR_REMOTE_DISCONNECT")

    agent.release_xfer_handle = fail_release

    with pytest.raises(RuntimeError):
        transport._cleanup_transfer("obj", [], "h1", "a", False, blocks)
    assert free_bytes(pool) == 64


def test_deferred_cleanup_frees_pool_blocks(transport, pool):
    """A cleanup that garbage collection runs under the cache lock keeps its blocks."""
    agent = transport._nixl_agent
    _, blocks = pool.allocate_regions([32])
    start_transfer(transport, "a", "h1")

    with transport._cache_guard():
        transport._cleanup_transfer("obj", [], "h1", "a", False, blocks)
        assert agent.released == []
        assert free_bytes(pool) == 32

    assert agent.released == ["h1"]
    assert free_bytes(pool) == 64


def test_polled_transfer_keeps_pool_blocks_until_its_handle_is_released(
    transport, pool
):
    """Removing an agent polls its transfers. If garbage collection cleans one up
    meanwhile, its blocks must outlive the polling, which can still see it in flight.
    """
    agent = transport._nixl_agent
    transport._prepare_remote_agent("a", 1, b"meta")
    start_transfer(transport, "a", "h1")
    _, blocks = pool.allocate_regions([32])
    check_xfer_state = agent.check_xfer_state
    during_polling = []

    def check_and_collect(handle):
        if not during_polling:
            # As if garbage collection dropped the abandoned fetch request here.
            transport._cleanup_transfer("obj", [], "h1", "a", False, blocks)
            during_polling.append((list(agent.released), free_bytes(pool)))
        return check_xfer_state(handle)

    agent.check_xfer_state = check_and_collect

    # A new metadata version removes the agent, which polls h1 first.
    transport._prepare_remote_agent("a", 2, b"meta")

    assert during_polling == [([], 32)]
    assert agent.released == ["h1"]
    assert free_bytes(pool) == 64


def test_cleanup_during_a_pool_call_does_not_deadlock(monkeypatch, transport, pool):
    """Garbage collection can run a cleanup while the pool's lock is held.

    The lock is not reentrant, so the cleanup's own free has to wait until the
    pool call returns.
    """
    agent = transport._nixl_agent
    _, collected_blocks = pool.allocate_regions([16])
    _, blocks = pool.allocate_regions([16])
    start_transfer(transport, "a", "h1")
    merge_free_blocks = nixl_memory_pool._merge_free_blocks
    collected = []

    def merge_and_collect(free_list):
        merge_free_blocks(free_list)
        if not collected:
            collected.append(True)
            # As if garbage collection dropped an abandoned fetch request here.
            transport._cleanup_transfer("obj", [], "h1", "a", False, collected_blocks)

    monkeypatch.setattr(nixl_memory_pool, "_merge_free_blocks", merge_and_collect)

    thread = threading.Thread(
        target=transport._free_pool_blocks, args=(blocks,), daemon=True
    )
    thread.start()
    thread.join(timeout=10)

    assert not thread.is_alive(), "freeing pool blocks deadlocked"
    assert collected == [True]
    assert agent.released == ["h1"]
    assert free_bytes(pool) == 64


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
