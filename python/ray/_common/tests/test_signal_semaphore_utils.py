"""Tests for Ray test utility classes.

This module contains pytest-based tests for SignalActor and Semaphore classes
from ray._common.test_utils. These test utility classes are used for coordination
and synchronization in Ray tests.
"""

import sys
import time

import pytest

import ray
from ray._common.test_utils import Semaphore, SignalActor, wait_for_condition


@pytest.fixture(scope="module")
def ray_init():
    """Initialize Ray for the test module."""
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_signal_actor_basic(ray_init):
    """Test basic SignalActor functionality - send and wait operations."""
    signal = SignalActor.remote()

    # Test initial state
    assert ray.get(signal.cur_num_waiters.remote()) == 0

    # Test send and wait
    ray.get(signal.send.remote())
    signal.wait.remote()
    assert ray.get(signal.cur_num_waiters.remote()) == 0


def test_signal_actor_multiple_waiters(ray_init):
    """Test SignalActor with multiple waiters and signal clearing."""
    signal = SignalActor.remote()

    # Create multiple waiters
    for _ in range(3):
        signal.wait.remote()

    # Check number of waiters
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 3)

    # Send signal and wait for all waiters
    ray.get(signal.send.remote())

    # Verify all waiters are done
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 0)

    # check that .wait() doesn't block if the signal is already sent
    ray.get(signal.wait.remote())

    assert ray.get(signal.cur_num_waiters.remote()) == 0

    # clear the signal
    ray.get(signal.send.remote(clear=True))
    signal.wait.remote()
    # Verify all waiters are done
    wait_for_condition(lambda: ray.get(signal.cur_num_waiters.remote()) == 1)

    ray.get(signal.send.remote())


def test_semaphore_basic(ray_init):
    """Test basic Semaphore functionality - acquire, release, and lock status."""
    sema = Semaphore.remote(value=2)

    # Test initial state
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is False)

    # Test acquire and release
    ray.get(sema.acquire.remote())
    ray.get(sema.acquire.remote())
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is True)

    ray.get(sema.release.remote())
    ray.get(sema.release.remote())
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is False)


def test_semaphore_concurrent(ray_init):
    """Test Semaphore with concurrent workers to verify resource limiting."""
    sema = Semaphore.remote(value=2)

    def worker():
        ray.get(sema.acquire.remote())
        time.sleep(0.1)
        ray.get(sema.release.remote())

    # Create multiple workers
    _ = [worker() for _ in range(4)]

    # Verify semaphore is not locked
    wait_for_condition(lambda: ray.get(sema.locked.remote()) is False)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
