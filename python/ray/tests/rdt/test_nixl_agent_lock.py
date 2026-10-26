"""Unit tests for removing NIXL remote agents while transfers on them are in flight.

These use a fake NIXL agent, so they run without NIXL or GPUs.
"""

import sys
import threading
import time

import pytest

from ray.experimental.rdt import nixl_tensor_transport
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
