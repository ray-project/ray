"""Diagnostics for a stalled dashboard-agent asyncio event loop.

The dashboard agent runs a single asyncio loop that serves metric ingestion,
event aggregation, and the ``POST /api/jobs/`` submit handler, so a stall in
that loop hangs job submission while the dashboard head stays healthy on its
own loop.

``EventLoopMonitor`` watches the loop three ways:

* It observes loop lag (via :func:`enable_monitor_loop_lag`) and logs the lag
  and pending-task count when lag is high.
* A daemon thread watches a heartbeat the loop refreshes and dumps every
  thread's stack via :mod:`faulthandler` when the heartbeat goes stale,
  capturing the frame the loop thread is blocked in.
* On demand, it dumps every thread's stack when the process receives a signal
  (``SIGUSR2`` by default), so an operator -- or an external watchdog that
  detected the hang from outside the process -- can capture the blocked frame
  immediately, without waiting for the stall threshold.

The stack-dump watchdog runs on its own thread on purpose: a fully blocked loop
cannot run an asyncio callback or fire an asyncio timeout, so an in-loop monitor
would be wedged too. It is read-only and never cancels or restarts anything.
"""

import asyncio
import faulthandler
import logging
import os
import signal
import sys
import threading
import time
from typing import Optional

from ray._common.utils import get_or_create_event_loop
from ray._private.async_utils import enable_monitor_loop_lag
from ray._private.ray_constants import env_bool, env_float

logger = logging.getLogger(__name__)


# Diagnostic-only instrumentation; off by default. Opt in with
# RAY_DASHBOARD_AGENT_LOOP_MONITOR_ENABLED=1.
EVENT_LOOP_MONITOR_ENABLED = env_bool("RAY_DASHBOARD_AGENT_LOOP_MONITOR_ENABLED", False)
# How often the loop-lag monitor samples (and the loop refreshes its heartbeat).
_SAMPLE_INTERVAL_S = env_float("RAY_DASHBOARD_AGENT_LOOP_MONITOR_INTERVAL_S", 0.25)
# Log a warning when observed loop lag exceeds this.
_LAG_WARN_THRESHOLD_S = env_float("RAY_DASHBOARD_AGENT_LOOP_LAG_WARN_THRESHOLD_S", 1.0)
# Dump all thread stacks once the loop has been blocked continuously this long.
_STALL_DUMP_THRESHOLD_S = env_float(
    "RAY_DASHBOARD_AGENT_LOOP_STALL_DUMP_THRESHOLD_S", 10.0
)
# Minimum gap between successive dumps so a long stall does not spam the log.
_DUMP_COOLDOWN_S = env_float("RAY_DASHBOARD_AGENT_LOOP_DUMP_COOLDOWN_S", 60.0)
# Signal that triggers an immediate all-thread stack dump on demand. Defaults
# to SIGUSR2, which is unused elsewhere in Ray (Tune already claims SIGUSR1).
# Set RAY_DASHBOARD_AGENT_LOOP_DUMP_SIGNAL to another signal name, or to an
# empty value to disable the handler.
_DUMP_SIGNAL_NAME = os.environ.get(
    "RAY_DASHBOARD_AGENT_LOOP_DUMP_SIGNAL", "SIGUSR2"
).strip()


class EventLoopMonitor:
    """Watches the dashboard-agent event loop for stalls."""

    def __init__(
        self,
        component: str = "dashboard_agent",
        *,
        sample_interval_s: float = _SAMPLE_INTERVAL_S,
        lag_warn_threshold_s: float = _LAG_WARN_THRESHOLD_S,
        stall_dump_threshold_s: float = _STALL_DUMP_THRESHOLD_S,
        dump_cooldown_s: float = _DUMP_COOLDOWN_S,
    ):
        self._component = component
        self._sample_interval_s = sample_interval_s
        self._lag_warn_threshold_s = lag_warn_threshold_s
        self._stall_dump_threshold_s = stall_dump_threshold_s
        self._dump_cooldown_s = dump_cooldown_s

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        # Last time the loop proved it was alive (monotonic seconds). Written by
        # the loop, read by the watchdog thread; a bare float access is atomic
        # under CPython, so no lock is needed.
        self._last_beat = time.monotonic()
        self._last_dump = 0.0
        self._stop = threading.Event()
        self._watchdog: Optional[threading.Thread] = None
        # Signal number the on-demand dump handler is registered on, if any.
        self._dump_signum: Optional[int] = None

    def start(self) -> None:
        """Start the monitor. Must be called from within the agent's loop."""
        self._loop = get_or_create_event_loop()
        # Start the stall clock at activation, not at construction time.
        self._last_beat = time.monotonic()

        enable_monitor_loop_lag(
            self._on_lag, interval_s=self._sample_interval_s, loop=self._loop
        )

        self._watchdog = threading.Thread(
            target=self._watchdog_loop,
            name="dashboard_agent_loop_watchdog",
            daemon=True,
        )
        self._watchdog.start()
        self._register_dump_signal()
        logger.info(
            "[EventLoopMonitor] watching %s event loop "
            "(lag_warn=%.1fs, stall_dump=%.1fs)",
            self._component,
            self._lag_warn_threshold_s,
            self._stall_dump_threshold_s,
        )

    def _on_lag(self, lag_s: float) -> None:
        # Runs on the loop, so reaching here proves the loop is alive.
        self._last_beat = time.monotonic()
        if lag_s >= self._lag_warn_threshold_s:
            try:
                pending = len(asyncio.all_tasks(self._loop))
            except Exception:
                pending = -1
            logger.warning(
                "[EventLoopMonitor] %s event loop lag %.3fs (pending_tasks=%d); "
                "job submission and metric/event export run on this loop.",
                self._component,
                lag_s,
                pending,
            )

    def _watchdog_loop(self) -> None:
        while not self._stop.wait(self._sample_interval_s):
            # If the loop is gone (shutdown/tests), stop rather than fire on a
            # heartbeat that will never advance again.
            if self._loop is not None and self._loop.is_closed():
                break
            stalled_for = time.monotonic() - self._last_beat
            if stalled_for < self._stall_dump_threshold_s:
                continue
            now = time.monotonic()
            if now - self._last_dump < self._dump_cooldown_s:
                continue
            self._last_dump = now
            self._dump_stacks(stalled_for)

    def _dump_stacks(self, stalled_for: float) -> None:
        logger.warning(
            "[EventLoopMonitor] %s event loop blocked for ~%.1fs (threshold "
            "%.1fs); dumping all thread stacks.",
            self._component,
            stalled_for,
            self._stall_dump_threshold_s,
        )
        # faulthandler writes Python frames for every thread, including the
        # blocked loop thread, to stderr, which the agent redirects to its log.
        try:
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        except Exception:
            logger.exception("[EventLoopMonitor] failed to dump thread stacks")

    def _register_dump_signal(self) -> None:
        """Install an on-demand stack-dump handler on the configured signal.

        Uses :func:`faulthandler.register`, whose C-level handler still fires
        when the loop thread is hard-blocked holding the GIL -- exactly the
        case the watchdog exists for -- so an external caller can force a dump
        at will. ``chain=False`` because ``SIGUSR2``'s default action is to
        kill the process, which must not happen here.

        Signal handlers can only be registered on the main thread; if the loop
        runs elsewhere this logs and skips rather than failing start-up.
        """
        if not _DUMP_SIGNAL_NAME:
            return
        signum = getattr(signal, _DUMP_SIGNAL_NAME, None)
        if signum is None:
            # e.g. SIGUSR2 does not exist on Windows.
            logger.debug(
                "[EventLoopMonitor] signal %r unavailable; "
                "on-demand stack dump disabled.",
                _DUMP_SIGNAL_NAME,
            )
            return
        try:
            faulthandler.register(
                signum, file=sys.stderr, all_threads=True, chain=False
            )
        except (ValueError, RuntimeError) as e:
            logger.warning(
                "[EventLoopMonitor] could not register %s dump handler "
                "(must run on the main thread): %s",
                _DUMP_SIGNAL_NAME,
                e,
            )
            return
        self._dump_signum = signum
        logger.info(
            "[EventLoopMonitor] send %s to pid %d to dump all thread stacks.",
            _DUMP_SIGNAL_NAME,
            os.getpid(),
        )

    def _unregister_dump_signal(self) -> None:
        if self._dump_signum is None:
            return
        try:
            faulthandler.unregister(self._dump_signum)
        except Exception:
            logger.exception("[EventLoopMonitor] failed to unregister dump signal")
        self._dump_signum = None

    def stop(self) -> None:
        self._stop.set()
        self._unregister_dump_signal()
        if self._watchdog is not None and self._watchdog.is_alive():
            self._watchdog.join(timeout=1.0)
