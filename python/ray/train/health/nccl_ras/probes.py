"""Probes of the NCCL hang detector. ``NCCLRASProbe`` polls NCCL RAS during
training, and the others capture a hang's diagnostics. Each one wraps a helper
in ``utils``, and passes its ``timeout_s`` to the command it runs."""
import os
from typing import Optional

from ray.train.health.nccl_ras.utils import (
    RASQueryError,
    dump_flight_recorder,
    dump_stack_trace,
    run_ncclras,
    run_nvidia_smi,
)
from ray.train.health.probe import NodeProbe, ProbeResult, WorkerProbe
from ray.train.v2._internal.constants import NCCL_RAS_ADDR_ENV_VAR

# --------------------------
# NodeProbes
# --------------------------


class NCCLRASProbe(NodeProbe):
    """Queries NCCL RAS on its node with ``ncclras -f json``, for the report
    in ``detail``. Every node sees the whole RAS mesh. After a fatal error,
    such as a missing binary, it returns that error without querying again."""

    fmt = "json"
    # Over ncclras's own default -t of 5.
    timeout_s = 8.0

    def __init__(self):
        # Read on the controller, which creates the probe and has the driver's
        # NCCL_RAS_ADDR.
        self._ras_addr = os.environ.get(NCCL_RAS_ADDR_ENV_VAR)
        self._fatal_error: Optional[RASQueryError] = None

    def poll(self) -> ProbeResult:
        if self._fatal_error is not None:
            return ProbeResult(error=self._fatal_error)
        try:
            report = run_ncclras(self.fmt, self.timeout_s, self._ras_addr)
        except RASQueryError as e:
            if e.fatal:
                self._fatal_error = e
            raise
        return ProbeResult(detail=report)


class NCCLRASTextProbe(NCCLRASProbe):
    """Queries NCCL RAS on its node for its human-readable report, with
    ``ncclras -f text``."""

    fmt = "text"


class NvidiaSmiProbe(NodeProbe):
    """Snapshots the node's GPUs with ``nvidia-smi -q``, in ``detail``."""

    timeout_s = 30.0

    def poll(self) -> ProbeResult:
        return run_nvidia_smi(self.timeout_s)


# --------------------------
# WorkerProbes
# --------------------------


class FlightRecorderProbe(WorkerProbe):
    """Dumps the worker's PyTorch Flight Recorder buffer as JSON, in
    ``detail``."""

    # The dump takes no timeout, so this only bounds how long Ray Train waits.
    timeout_s = 30.0

    def poll(self) -> ProbeResult:
        return dump_flight_recorder()


class StackTraceProbe(WorkerProbe):
    """Dumps the worker's native and Python stacks, in ``detail``. If
    ``py-spy`` is denied ptrace, it retries with passwordless ``sudo``, and
    without ``py-spy`` it dumps the Python stacks only."""

    timeout_s = 30.0

    def poll(self) -> ProbeResult:
        # Shared by py-spy, the sudo check and the sudo retry.
        return ProbeResult(detail=dump_stack_trace(self.timeout_s))
