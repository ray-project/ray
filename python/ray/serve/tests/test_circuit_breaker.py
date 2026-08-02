from __future__ import annotations

import asyncio
import threading

import pytest

from ray.serve._private.circuit_breaker import CircuitBreakerMiddleware


@pytest.mark.asyncio
async def test_closed_to_open_and_half_open_transition():
    cb = CircuitBreakerMiddleware(error_threshold=2, cooldown_seconds=0.1)

    # Two consecutive failures -> open
    cb.before_call(); cb.record_failure()
    cb.before_call(); cb.record_failure()
    assert cb.state.state == "open"

    # Calls should fail fast while open
    with pytest.raises(RuntimeError):
        cb.before_call()

    # After cooldown -> half_open, one success closes
    await asyncio.sleep(0.11)
    cb.before_call()
    assert cb.state.state == "half_open"
    cb.record_success()
    assert cb.state.state == "closed"


@pytest.mark.asyncio
async def test_half_open_failure_reopens():
    cb = CircuitBreakerMiddleware(error_threshold=1, cooldown_seconds=0.0)

    # Open directly
    cb.before_call(); cb.record_failure()
    assert cb.state.state == "open"

    # Next call triggers half_open then failure brings it back to open
    cb.before_call()
    assert cb.state.state == "half_open"
    cb.record_failure()
    assert cb.state.state == "open"


def test_only_one_concurrent_half_open_probe_admitted():
    cb = CircuitBreakerMiddleware(error_threshold=1, cooldown_seconds=0.0)

    # Open directly, then let cooldown elapse immediately (cooldown=0).
    cb.before_call()
    cb.record_failure()
    assert cb.state.state == "open"

    admitted = 0
    rejected = 0
    lock = threading.Lock()
    start = threading.Barrier(16)

    def worker():
        nonlocal admitted, rejected
        start.wait()
        try:
            cb.before_call()
        except RuntimeError:
            with lock:
                rejected += 1
            return
        with lock:
            admitted += 1

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one caller may win the open->half_open transition and become
    # the probe; every other concurrent caller must fail fast.
    assert admitted == 1
    assert rejected == 15
    assert cb.state.state == "half_open"
