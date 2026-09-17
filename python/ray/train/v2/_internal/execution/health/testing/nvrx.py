"""Inject real process-level faults with NVIDIA's Resiliency Extension.

NVRx ships a fault menu that is already used to test its own fault tolerance,
so we use it rather than writing our own: every entry is software, reproducible
from a seed, and picks its target ranks the same way each run.

    from ray.train.v2._internal.execution.health.testing import nvrx

    def train_func(config):
        nvrx.inject(nvrx.GPU_SLEEP, delay_s=120, keep_alive=1, seed=7)
        for step, batch in enumerate(loader):
            ...

``keep_alive`` protects the low ranks, so rank 0 survives to keep reporting --
which is what makes the fault observable rather than just fatal.

Faults worth wiring first, and what each one tests:

===============  ======================================  =======================
fault            what it does                            what it exercises
===============  ======================================  =======================
``GPU_SLEEP``    ``torch.cuda._sleep(1 << 62)``          straggler, then hang:
                                                         RAS op counts freeze
``LOCK_GIL``     catastrophic regex backtracking         process alive, Python
                                                         frozen -- the worker
                                                         stops answering while
                                                         the node is healthy
``SIGSTOP``      freezes the process                     same shape, no error
                                                         anywhere
``GPU_ERROR``    out-of-bounds index, device assert      sticky CUDA error
``SEGFAULT``     ``ctypes.string_at(1)``                 hard worker death
``OS_ABORT``     ``os.abort()``                          hard worker death
===============  ======================================  =======================

``LOCK_GIL`` and ``SIGSTOP`` are the two the ``NodeMonitor`` exists for: the
worker cannot describe itself, nothing raises, and node health is the only
thing still talking.
"""
import logging
import os
from typing import Any, Callable, Optional, Sequence, Tuple, Union

logger = logging.getLogger(__name__)

# Names rather than the enum, so importing this module never requires NVRx.
GPU_SLEEP = "GPU_SLEEP"
GPU_ERROR = "GPU_ERROR"
LOCK_GIL = "LOCK_GIL"
SIGSTOP = "SIGSTOP"
SIGKILL = "SIGKILL"
SEGFAULT = "SEGFAULT"
OS_ABORT = "OS_ABORT"
WORKLOAD_EXC = "WORKLOAD_EXC"

#: Faults that need a CUDA device. The rest run anywhere.
NEEDS_CUDA = frozenset({GPU_SLEEP, GPU_ERROR})

#: Faults that leave the process alive but unable to answer. These are the
#: interesting ones for node-level health, and they need no GPU.
SILENT_FAULTS = frozenset({LOCK_GIL, SIGSTOP})


class NVRxUnavailable(RuntimeError):
    """NVRx is not installed, so a real fault cannot be injected."""


def available() -> bool:
    """Whether real injection can run in this process."""
    try:
        import nvidia_resiliency_ext.shared_utils.inject_fault  # noqa: F401

        return True
    except ImportError:
        return False


def _fault_enum(name: str):
    from nvidia_resiliency_ext.shared_utils.inject_fault import Fault

    try:
        return Fault[name]
    except KeyError as e:
        raise ValueError(
            f"Unknown NVRx fault {name!r}. Known: {sorted(f.name for f in Fault)}"
        ) from e


def inject(
    fault: Union[str, Sequence[str]],
    *,
    delay_s: Union[float, Tuple[float, float]] = 60.0,
    num_faults: Union[int, Tuple[int, int]] = 1,
    keep_alive: int = 1,
    seed: int = 0,
    callback: Optional[Callable[[], Any]] = None,
    require: bool = True,
) -> bool:
    """Arm an NVRx fault on a subset of ranks. Returns whether it was armed.

    Call this once at the top of the training function on every rank; NVRx
    decides which ranks actually get the fault from ``seed``, ``num_faults``
    and ``keep_alive``, reading ``RANK`` and ``WORLD_SIZE`` from the
    environment, which Ray Train's torch backend sets.

    Args:
        fault: One fault name, or several to pick from.
        delay_s: Seconds before firing, or a ``(min, max)`` range. Give the run
            enough time to reach steady state, or the detector will be
            measuring startup.
        num_faults: How many ranks to hit, or a ``(min, max)`` range.
        keep_alive: Ranks below this index are never targeted. Keep at least
            one so there is a healthy peer to compare against -- without it a
            peer-relative check has nothing to compare to.
        seed: Makes the choice of rank and fault reproducible.
        callback: Run on the targeted rank immediately before the fault fires.
            Use it to mark the injection in the run's own logs, so the detector's
            latency can be measured against a known start time.
        require: Raise if NVRx is missing. Pass ``False`` to make injection a
            no-op where it is unavailable.

    Raises:
        NVRxUnavailable: NVRx is not installed and ``require`` is set.
    """
    names = [fault] if isinstance(fault, str) else list(fault)
    if not names:
        raise ValueError("inject() needs at least one fault")

    if not available():
        if require:
            raise NVRxUnavailable(
                "nvidia-resiliency-ext is not installed, so "
                f"{names} cannot be injected. `pip install nvidia-resiliency-ext`."
            )
        logger.warning("NVRx not installed; skipping injection of %s.", names)
        return False

    needs_cuda = [n for n in names if n in NEEDS_CUDA]
    if needs_cuda and not _cuda_available():
        if require:
            raise NVRxUnavailable(
                f"{needs_cuda} need a CUDA device. On a CPU-only runner use "
                f"{sorted(SILENT_FAULTS)} instead, which reproduce the same "
                "'worker stops answering' shape."
            )
        logger.warning("No CUDA device; skipping injection of %s.", needs_cuda)
        return False

    from nvidia_resiliency_ext.shared_utils.inject_fault import inject_fault

    rank = os.getenv("RANK", "?")
    logger.warning(
        "[fault-injection] arming %s on up to %s rank(s) (this is rank %s, "
        "keep_alive=%s, seed=%s, delay=%s)",
        names,
        num_faults,
        rank,
        keep_alive,
        seed,
        delay_s,
    )
    inject_fault(
        faults=tuple(_fault_enum(n) for n in names),
        num_faults=num_faults,
        keep_alive=keep_alive,
        delay=float(delay_s) if isinstance(delay_s, (int, float)) else delay_s,
        seed=seed,
        callback=callback,
    )
    return True


def _cuda_available() -> bool:
    try:
        import torch

        return torch.cuda.is_available()
    except Exception:
        return False
