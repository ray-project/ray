import asyncio
import logging
import sys
import threading
import time

import pytest

from ray.dashboard.event_loop_monitor import EventLoopMonitor


def test_on_lag_warns_and_refreshes_heartbeat(caplog):
    """Lag above the threshold logs a warning and refreshes the heartbeat."""
    monitor = EventLoopMonitor(lag_warn_threshold_s=1.0)
    monitor._loop = asyncio.new_event_loop()
    try:
        monitor._last_beat = 0.0  # pretend stale
        with caplog.at_level(logging.WARNING):
            monitor._on_lag(2.0)
        assert monitor._last_beat > 0.0  # heartbeat refreshed
        assert any("event loop lag" in r.getMessage().lower() for r in caplog.records)
    finally:
        monitor._loop.close()


def test_on_lag_below_threshold_is_quiet_but_still_beats(caplog):
    monitor = EventLoopMonitor(lag_warn_threshold_s=5.0)
    monitor._loop = asyncio.new_event_loop()
    try:
        monitor._last_beat = 0.0
        with caplog.at_level(logging.WARNING):
            monitor._on_lag(0.1)
        assert monitor._last_beat > 0.0
        assert not any(
            "event loop lag" in r.getMessage().lower() for r in caplog.records
        )
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
