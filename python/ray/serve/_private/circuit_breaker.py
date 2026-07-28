from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class CircuitBreakerState:
    failure_count: int = 0
    state: str = "closed"  # closed, open, half_open
    opened_at: float = 0.0


class CircuitBreakerMiddleware:
    """Simple circuit breaker to guard calls to deployment handles.

    - closed: calls pass through; consecutive failures increment counter.
    - open: calls fail fast until cooldown elapses.
    - half_open: allow a probe call; success -> closed/reset, failure -> open.
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

    def _now(self) -> float:
        return time.monotonic()

    def before_call(self) -> None:
        if self._state.state == "open":
            if self._now() - self._state.opened_at >= self._cooldown:
                self._state.state = "half_open"
                self._state.failure_count = 0
            else:
                raise RuntimeError("CircuitBreaker: open")

    def record_success(self) -> None:
        if self._state.state in ("half_open", "open"):
            self._state = CircuitBreakerState()
        else:
            self._state.failure_count = 0

    def record_failure(self) -> None:
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
        return self._state
