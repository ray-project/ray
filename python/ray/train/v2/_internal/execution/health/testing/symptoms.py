"""Report the numbers a fault produces, without the fault being real.

A detector consumes a symptom, and every symptom in this loop is a number the
training loop reports. So a unit test can inject the number directly and still
be testing the real detector -- what it cannot test is whether a real fault
produces that number, which is a hardware question.

Use this for the fast tests. Use :mod:`~.nvrx` and :mod:`~.dcgm` for the slow
ones that check the causal chain.

:class:`CudaHang` and :class:`CollectiveDesync` are the exception: they produce
real faults on real GPUs and need nothing but torch. They exist because
``nvidia-resiliency-ext`` ships wheels only for cp312/cp314 on
``manylinux_2_39``, while Ray's GPU image is Python 3.10 on Ubuntu 22.04 --
two independent mismatches, so NVRx does not pip-install there. These are the
same two faults, without the dependency.

    from ray.train.v2._internal.execution.health.testing import symptoms

    def train_func(config):
        fault = symptoms.Straggler(rank=4, slowdown=2.4)
        for step, batch in enumerate(loader):
            with fault.step(step):          # sleeps if this is the target rank
                loss = train_step(batch)
"""
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ray.train.v2._internal.execution.health.report import report


def _rank() -> int:
    import os

    return int(os.getenv("RANK", "0"))


@dataclass
class Symptom:
    """Base: applies to one rank, from one step onward."""

    rank: int = 0
    start_step: int = 0
    #: Report through ``health.report()`` as well as producing the effect.
    emit: bool = True

    def targets(self, step: int) -> bool:
        return _rank() == self.rank and step >= self.start_step

    def _report(self, metrics: Dict[str, Any], step: int) -> None:
        if self.emit:
            report(metrics, step=step)


@dataclass
class Straggler(Symptom):
    """This rank takes ``slowdown`` times as long per step.

    Produces the effect for real -- it sleeps -- so a collective built on top
    of it skews the way it would under a degrading GPU, and the reported step
    time is measured rather than fabricated.
    """

    slowdown: float = 2.0
    #: Where the extra time goes. "compute" looks like a bad GPU; "dataload"
    #: looks like a starved shard, and must not evict the node.
    phase: str = "compute"
    pipeline_stage: Optional[int] = None

    @contextmanager
    def step(self, step: int):
        started = time.perf_counter()
        yield
        elapsed = time.perf_counter() - started
        if self.targets(step):
            extra = elapsed * (self.slowdown - 1.0)
            time.sleep(max(extra, 0.0))
            total = elapsed + extra
        else:
            total = elapsed

        metrics: Dict[str, Any] = {"step_time_s": total}
        if self.phase == "compute":
            metrics["compute_time_s"] = total
        else:
            # The GPU did its usual work; the wait was elsewhere.
            metrics["compute_time_s"] = elapsed
        if self.pipeline_stage is not None:
            metrics["pipeline_stage"] = self.pipeline_stage
        self._report(metrics, step)


@dataclass
class WanderingStraggler(Symptom):
    """A different rank is slow each step: GC pauses, noisy neighbours.

    The control for :class:`Straggler` -- identical symptom, and a detector
    that evicts on it is wrong.
    """

    slowdown: float = 2.0
    world_size: int = 1

    def targets(self, step: int) -> bool:
        if self.world_size <= 0:
            return False
        return _rank() == (step % self.world_size) and step >= self.start_step

    @contextmanager
    def step(self, step: int):
        started = time.perf_counter()
        yield
        elapsed = time.perf_counter() - started
        if self.targets(step):
            extra = elapsed * (self.slowdown - 1.0)
            time.sleep(max(extra, 0.0))
            elapsed += extra
        self._report({"step_time_s": elapsed, "compute_time_s": elapsed}, step)


@dataclass
class NumericalFault(Symptom):
    """This rank's gradients go wrong while its peers' stay healthy.

    The observable signature of silent corruption. ``everywhere`` flips it into
    the control case -- every rank anomalous on the same step, which is a bad
    batch and must not evict anything.
    """

    bad_value: float = float("nan")
    healthy_value: float = 1.8
    metric: str = "grad_norm"
    everywhere: bool = False
    only_step: Optional[int] = None

    def targets(self, step: int) -> bool:
        if self.only_step is not None and step != self.only_step:
            return False
        if step < self.start_step:
            return False
        return self.everywhere or _rank() == self.rank

    def observe(self, step: int, value: Optional[float] = None) -> float:
        """Report this step's value, substituting the fault when targeted."""
        reported = (
            self.bad_value
            if self.targets(step)
            else (self.healthy_value if value is None else value)
        )
        self._report({self.metric: reported}, step)
        return reported


@dataclass
class CudaHang(Symptom):
    """Wedge this rank's CUDA stream, the way NVRx's ``GPU_SLEEP`` does.

    ``torch.cuda._sleep`` queues a kernel that spins for a number of clock
    cycles; ``1 << 62`` is several human lifetimes, so the stream never drains.
    The rank stops advancing its collectives while its process stays alive and
    its node stays healthy -- which is the hang the detector is for.

    Needs a GPU and nothing else. No NVRx, no extra wheel.
    """

    delay_s: float = 0.0
    cycles: int = 1 << 62
    _fired: bool = field(default=False, init=False)

    def fire(self, step: int) -> bool:
        """Wedge the stream if this rank is the target. Returns whether it did."""
        if self._fired or not self.targets(step):
            return False
        import torch

        if not torch.cuda.is_available():
            raise RuntimeError("CudaHang needs a CUDA device")
        self._fired = True
        if self.delay_s:
            time.sleep(self.delay_s)
        device = torch.cuda.current_device()
        import logging

        logging.getLogger(__name__).critical(
            "[fault-injection] wedging CUDA stream on rank %s device %s at step %s",
            _rank(),
            device,
            step,
        )
        torch.cuda._sleep(self.cycles)
        return True


@dataclass
class CollectiveDesync(Symptom):
    """This rank skips a collective its peers are waiting on.

    The purest input for a RAS-based detector: the skipped op leaves this rank's
    collective count one behind its peers', every other rank blocks inside the
    all-reduce, and nothing advances again. That is exactly the shape
    ``mismatched_comms`` keys on -- counts differ, every rank still RUNNING.

    Needs torch.distributed already initialized, which Ray Train's torch backend
    does. Nothing else.

        fault = CollectiveDesync(rank=3, start_step=50)
        for step, batch in enumerate(loader):
            ...
            if not fault.maybe_skip(step):
                dist.all_reduce(grads)
    """

    def maybe_skip(self, step: int) -> bool:
        """Whether this rank should skip the collective at ``step``."""
        if not self.targets(step):
            return False
        import logging

        logging.getLogger(__name__).critical(
            "[fault-injection] rank %s skipping the collective at step %s; its "
            "peers will block",
            _rank(),
            step,
        )
        return True


@dataclass
class Hang(Symptom):
    """This rank stops stepping, without dying.

    The pure-Python stand-in for NVRx's ``GPU_SLEEP`` on a runner with no GPU.
    The rank keeps its last reported value and its ``step`` stops advancing,
    which is what a no-progress check keys on.
    """

    after_steps: int = 10
    duration_s: float = 3600.0
    _hung: bool = field(default=False, init=False)

    @contextmanager
    def step(self, step: int):
        yield
        if self.targets(step) and step >= self.after_steps and not self._hung:
            self._hung = True
            time.sleep(self.duration_s)
        self._report({"step_time_s": 0.4}, step)
