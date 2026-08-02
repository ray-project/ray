from __future__ import annotations

import threading
import time
from dataclasses import dataclass, replace


@dataclass
class CircuitBreakerState:
    failure_count: int = 0
    state: str = "closed"  # closed, open, half_open
    opened_at: float = 0.0


class CircuitBreakerMiddleware:
    """Simple circuit breaker to guard calls to deployment handles.

    - closed: calls pass through; consecutive failures increment counter.
    - open: calls fail fast until cooldown elapses.
    - half_open: exactly one probe call is allowed through; success ->
      closed/reset, failure -> open. Concurrent callers during half_open
      fail fast until that probe resolves.

    All state reads/transitions are guarded by a lock so that concurrent
    callers (e.g. multiple in-flight requests on the same handle) cannot
    race past the cooldown check and each believe they are the half-open
    probe.
    """

    def __init__(
        self,
        *,
        error_threshold: int = 5,
        cooldown_seconds: float = 10.0,
    ) -> None:
        self._threshold = max(1, error_threshold)
        self._cooldown = max(0.0, cooldown_seconds)
        self._state = CircuitBreakerState()
        self._lock = threading.Lock()

    def _now(self) -> float:
        return time.monotonic()

    def before_call(self) -> None:
        with self._lock:
            if self._state.state == "open":
                if self._now() - self._state.opened_at >= self._cooldown:
                    # This caller wins the transition and becomes the sole
                    # half-open probe. Any other caller observes "half_open"
                    # below (never "open" again) until the probe resolves.
                    self._state.state = "half_open"
                    self._state.failure_count = 0
                    return
                raise RuntimeError("CircuitBreaker: open")
            if self._state.state == "half_open":
                # A probe is already in flight; reject until it resolves via
                # record_success/record_failure.
                raise RuntimeError("CircuitBreaker: half-open probe in flight")

    def record_success(self) -> None:
        with self._lock:
            if self._state.state in ("half_open", "open"):
                self._state = CircuitBreakerState()
            else:
                self._state.failure_count = 0

    def record_failure(self) -> None:
        with self._lock:
            if self._state.state == "half_open":
                self._state.state = "open"
                self._state.opened_at = self._now()
                self._state.failure_count = 1
                return
            self._state.failure_count += 1
            if self._state.failure_count >= self._threshold:
                self._state.state = "open"
                self._state.opened_at = self._now()

    @property
    def state(self) -> CircuitBreakerState:
        with self._lock:
            return replace(self._state)
