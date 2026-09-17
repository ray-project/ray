"""``ray.train.health.report()`` -- signals only the training loop can produce.

This is the half of Collect that no vendor agent can supply. DCGM, NCCL RAS and
NVSentinel see the hardware; none of them can see that this rank's gradient
checksum disagrees with its data-parallel peers, or that its step time has
doubled while its op counts keep advancing. Those live inside the training loop
and nowhere else.

Usage is one line:

    import ray.train.health as health

    def train_func(config):
        for step, batch in enumerate(loader):
            ...
            health.report({"step_time_s": dt, "grad_norm": gn}, step=step)

Values are last-write-wins and the accumulator is never cleared by a snapshot.
A rank that stops reporting keeps its last reading rather than vanishing, so a
stalled rank is visible as a ``step`` that stops advancing -- which is a signal
-- instead of as missing data, which is an absence a policy cannot reason about.
"""
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReportSnapshot:
    """What the worker hands the controller on each poll."""

    metrics: Dict[str, Any]
    step: Optional[int]
    reported_at: float


class _Accumulator:
    """Written by the training thread, read by the worker's poll thread.

    The lock is held only for a dict update, so reporting costs the training
    loop nothing measurable. Collection must never perturb training.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._metrics: Dict[str, Any] = {}
        self._step: Optional[int] = None
        self._reported_at: float = 0.0

    def report(self, metrics: Dict[str, Any], step: Optional[int] = None) -> None:
        with self._lock:
            self._metrics.update(metrics)
            if step is not None:
                self._step = step
            self._reported_at = time.monotonic()

    def snapshot(self) -> ReportSnapshot:
        with self._lock:
            return ReportSnapshot(
                metrics=dict(self._metrics),
                step=self._step,
                reported_at=self._reported_at,
            )

    def clear(self) -> None:
        with self._lock:
            self._metrics.clear()
            self._step = None
            self._reported_at = 0.0


_accumulator = _Accumulator()


@DeveloperAPI
def report(metrics: Dict[str, Any], *, step: Optional[int] = None) -> None:
    """Report health signals from inside the training loop.

    Args:
        metrics: Signals for this rank, e.g. ``{"step_time_s": 0.42}``. Merged
            into whatever was reported before, last write wins.
        step: The training step these signals belong to. Evaluators use it both
            to align ranks with each other and to notice a rank that has
            stopped advancing.
    """
    if not isinstance(metrics, dict):
        raise TypeError(f"health.report() takes a dict of metrics, got {type(metrics)}")
    _accumulator.report(metrics, step)


def snapshot() -> ReportSnapshot:
    """Drain the accumulator for ``WorkerStatus.health``. Called on the worker."""
    return _accumulator.snapshot()


def reset() -> None:
    """Clear reported state. Called when a worker starts a new attempt."""
    _accumulator.clear()
