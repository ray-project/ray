"""CPU-only unit tests for the reader-writer guard that protects NIXL remote agents
from being removed while transfers on them are in flight."""

import sys
import threading
import time

import pytest

from ray.experimental.rdt.nixl_tensor_transport import _AgentGuard

TIMEOUT_S = 5


def run_in_thread(fn):
    t = threading.Thread(target=fn, daemon=True)
    t.start()
    return t


def test_readers_do_not_block_each_other():
    guard = _AgentGuard()
    keys = []
    t = run_in_thread(lambda: keys.append(guard.acquire_read("a")))
    t.join(TIMEOUT_S)
    keys.append(guard.acquire_read("a"))
    assert len(keys) == 2 and not t.is_alive()


def test_writer_waits_for_reader_on_another_thread():
    guard = _AgentGuard()
    key = guard.acquire_read("a")
    removed = threading.Event()

    def remove():
        with guard.write("a", wait_done=lambda handle: None):
            removed.set()

    t = run_in_thread(remove)
    assert not removed.wait(0.3)
    guard.release_read("a", key)
    assert removed.wait(TIMEOUT_S)
    t.join(TIMEOUT_S)


def test_writer_does_not_wait_for_other_agents():
    guard = _AgentGuard()
    guard.acquire_read("b")
    with guard.write("a", wait_done=lambda handle: None):
        pass


def test_writer_drains_own_transfers_first():
    guard = _AgentGuard()
    drained = []
    for handle in ("x", "y"):
        guard.set_handle("a", guard.acquire_read("a"), handle)

    with guard.write("a", wait_done=drained.append):
        assert sorted(drained) == ["x", "y"]
        assert not guard._reads["a"]


def test_thread_with_transfers_is_not_blocked_by_waiting_writer():
    guard = _AgentGuard()
    first = guard.acquire_read("a")
    writer_done = threading.Event()

    def remove():
        with guard.write("a", wait_done=lambda handle: None):
            pass
        writer_done.set()

    t = run_in_thread(remove)
    while not guard._waiting_writers["a"]:
        time.sleep(0.01)
    # Would deadlock if the waiting writer blocked this second read.
    second = guard.acquire_read("a")
    assert not writer_done.is_set()
    guard.release_read("a", first)
    guard.release_read("a", second)
    assert writer_done.wait(TIMEOUT_S)
    t.join(TIMEOUT_S)


def test_new_reader_waits_while_writer_is_active():
    guard = _AgentGuard()
    acquired = threading.Event()
    with guard.write("a", wait_done=lambda handle: None):
        t = run_in_thread(lambda: (guard.acquire_read("a"), acquired.set()))
        assert not acquired.wait(0.3)
    assert acquired.wait(TIMEOUT_S)
    t.join(TIMEOUT_S)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
