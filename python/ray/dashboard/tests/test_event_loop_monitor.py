import asyncio
import faulthandler
import logging
import signal
import sys
import threading
import time
from contextlib import contextmanager

import pytest

from ray.dashboard import event_loop_monitor
from ray.dashboard.event_loop_monitor import EventLoopMonitor


# pytest's ``caplog`` attaches to the root logger, but Ray sets
# ``propagate=False`` on the "ray" logger, so records emitted by
# ``ray.dashboard.*`` never reach the root handler under the dashboard test
# harness (they still show on stderr). Attach a handler to the module logger
# directly so capture is independent of propagation.
@contextmanager
def capture_records(logger, level=logging.WARNING):
    records = []

    class _ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _ListHandler()
    prev_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(level)
    try:
        yield records
    finally:
        logger.setLevel(prev_level)
        logger.removeHandler(handler)


def test_on_lag_warns_and_refreshes_heartbeat():
    """Lag above the threshold logs a warning and refreshes the heartbeat."""
    monitor = EventLoopMonitor(lag_warn_threshold_s=1.0)
    monitor._loop = asyncio.new_event_loop()
    try:
        monitor._last_beat = 0.0  # pretend stale
        with capture_records(event_loop_monitor.logger) as records:
            monitor._on_lag(2.0)
        assert monitor._last_beat > 0.0  # heartbeat refreshed
        assert any("event loop lag" in r.getMessage().lower() for r in records)
    finally:
        monitor._loop.close()


def test_on_lag_below_threshold_is_quiet_but_still_beats():
    monitor = EventLoopMonitor(lag_warn_threshold_s=5.0)
    monitor._loop = asyncio.new_event_loop()
    try:
        monitor._last_beat = 0.0
        with capture_records(event_loop_monitor.logger) as records:
            monitor._on_lag(0.1)
        assert monitor._last_beat > 0.0
        assert not any("event loop lag" in r.getMessage().lower() for r in records)
    finally:
        monitor._loop.close()


def test_watchdog_dumps_when_loop_heartbeat_is_stale(monkeypatch):
    """A stale heartbeat (loop blocked) triggers an out-of-loop stack dump."""
    monitor = EventLoopMonitor(
        sample_interval_s=0.01,
        stall_dump_threshold_s=0.02,
        dump_cooldown_s=0.0,
    )
    monitor._loop = asyncio.new_event_loop()
    dumped = threading.Event()
    monkeypatch.setattr(monitor, "_dump_stacks", lambda stalled_for: dumped.set())
    # Heartbeat never refreshed -> the watchdog must observe a stall and dump.
    monitor._last_beat = time.monotonic() - 1.0
    monitor._watchdog = threading.Thread(target=monitor._watchdog_loop, daemon=True)
    monitor._watchdog.start()
    try:
        assert dumped.wait(timeout=2.0), "watchdog did not dump on a stalled heartbeat"
    finally:
        monitor.stop()
        monitor._loop.close()


def test_watchdog_quiet_when_loop_is_healthy(monkeypatch):
    monitor = EventLoopMonitor(
        sample_interval_s=0.01,
        stall_dump_threshold_s=0.5,
        dump_cooldown_s=0.0,
    )
    monitor._loop = asyncio.new_event_loop()
    dumped = threading.Event()
    monkeypatch.setattr(monitor, "_dump_stacks", lambda stalled_for: dumped.set())
    monitor._watchdog = threading.Thread(target=monitor._watchdog_loop, daemon=True)
    monitor._watchdog.start()
    try:
        # Keep beating: heartbeat never gets older than the threshold.
        for _ in range(20):
            monitor._last_beat = time.monotonic()
            time.sleep(0.02)
        assert not dumped.is_set(), "watchdog dumped while the loop was healthy"
    finally:
        monitor.stop()
        monitor._loop.close()


def test_dump_signal_registers_and_unregisters(monkeypatch):
    """start() arms the on-demand dump signal; stop() disarms it."""
    if not hasattr(signal, "SIGUSR2"):
        pytest.skip("SIGUSR2 is not available on this platform")

    registered = {}
    monkeypatch.setattr(
        faulthandler,
        "register",
        lambda signum, **kwargs: registered.update(signum=signum, **kwargs),
    )
    monkeypatch.setattr(
        faulthandler,
        "unregister",
        lambda signum: registered.update(unregistered=signum),
    )

    monitor = EventLoopMonitor()
    monitor._register_dump_signal()
    assert registered["signum"] == signal.SIGUSR2
    assert registered["all_threads"] is True
    # chain=False so the dump does not fall through to SIGUSR2's default
    # action, which would terminate the agent.
    assert registered["chain"] is False
    assert monitor._dump_signum == signal.SIGUSR2

    monitor._unregister_dump_signal()
    assert registered["unregistered"] == signal.SIGUSR2
    assert monitor._dump_signum is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
