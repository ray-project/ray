"""NCCL RAS hang detection on ``ray.train.health``, built from the probes in
``ray.train.v2._internal.callbacks.nccl_ras`` (7/n).

It mirrors the merged ``NCCLRASCallback``:

- ``nccl_hang_check()``, an in-flight check. ``NCCLRASProbe`` runs
  ``ncclras -f json`` on every worker node's ``NodeMonitor``, and
  ``NCCLHangEvaluator`` keeps a per-communicator streak of polls in which a
  mismatched communicator made no progress. At the suspicion threshold it
  returns a ``Diagnose`` that logs NCCL's text report; at the confirm threshold,
  a ``Diagnose`` that captures the Flight Recorder, stack traces, ``nvidia-smi``
  and the RAS text report, then ``HangDiagnostics`` evicts a node with a faulty
  GPU, or reattempts.
- ``nccl_ras_preflight_check()``, a pre-flight check that evicts a node that
  can't run NCCL RAS.

Scripts using this module call ``harness.ship_by_value(nccl_ras_health)``,
because the controller and the workers can't import it.
"""
import logging
import math
import os
import re
import shutil
import subprocess
from collections import deque
from typing import Callable, Deque, Dict, List, Optional, Tuple

from ray.train.health import (
    Diagnose,
    Evaluator,
    Evict,
    HealthCheck,
    HealthState,
    NodeProbe,
    Noop,
    PeriodicHealthCheck,
    ProbeResult,
    Reattempt,
)
from ray.train.v2._internal.callbacks.nccl_ras import (
    FlightRecorderProbe,
    NCCLRASProbe,
    NCCLRASTextProbe,
    NvidiaSmiProbe,
    RASReport,
    StackTraceProbe,
    compute_report_op_diff,
    parse_ras_schema,
)

logger = logging.getLogger("ray.train")

_FIRST_SUSPICION_AFTER_S = 60.0
_HISTORY_MARGIN_POLLS = 10
_MIN_NCCL = (2, 28)


# ----------------------------------------------------------------------
# Pre-flight
# ----------------------------------------------------------------------
def _major_minor(version) -> Optional[Tuple[int, int]]:
    if isinstance(version, int):  # older torch: 22809 for 2.28.9
        return version // 10000, version // 100 % 100
    if isinstance(version, tuple):
        return tuple(version[:2])
    match = re.search(r"(\d+)\.(\d+)", version or "")
    return (int(match.group(1)), int(match.group(2))) if match else None


class NCCLRASReadyProbe(NodeProbe):
    """Whether this node can run NCCL RAS hang detection: an ``ncclras`` client
    and an NCCL of at least 2.28, the first with ``ncclras -f json``."""

    def poll(self) -> ProbeResult:
        problems = []
        if shutil.which("ncclras") is None:
            problems.append("ncclras not found")
        else:
            proc = subprocess.run(
                ["ncclras", "--version"], capture_output=True, text=True, timeout=10
            )
            client = _major_minor(proc.stdout + proc.stderr)
            if client is None or client < _MIN_NCCL:
                problems.append(f"ncclras {client} is older than 2.28")
        try:
            import torch

            nccl = _major_minor(torch.cuda.nccl.version())
            if nccl is None or nccl < _MIN_NCCL:
                problems.append(f"NCCL {nccl} is older than 2.28")
        except Exception as e:  # noqa: BLE001
            problems.append(f"can't read the NCCL version: {e}")
        return ProbeResult(
            metrics={"ready": float(not problems)},
            detail="; ".join(problems) or "ready",
        )


class EvictNodesNotReady(Evaluator):
    def evaluate(self, state: HealthState):
        results = state.probe_results.get("NCCLRASReadyProbe", {})
        not_ready = {
            node: r.detail or str(r.error)
            for node, r in results.items()
            if r.error is not None or not r.metrics.get("ready")
        }
        if not not_ready:
            return Noop()
        return Evict(
            reason=f"nodes can't run NCCL RAS: {not_ready}",
            target_nodes=sorted(not_ready),
        )


def nccl_ras_preflight_check() -> HealthCheck:
    return HealthCheck(
        probe_creator=lambda: [NCCLRASReadyProbe()],
        evaluator_creator=lambda: [EvictNodesNotReady()],
    )


# ----------------------------------------------------------------------
# GPU snapshot
# ----------------------------------------------------------------------
def _as_float(text: str) -> Optional[float]:
    try:
        return float(text.strip())
    except (TypeError, ValueError):
        return None


# Field names from real `nvidia-smi -q` output on an A10G.
_UNCORRECTABLE_KEYS = (
    "SRAM Uncorrectable Parity",
    "SRAM Uncorrectable SEC-DED",
    "DRAM Uncorrectable",
)
_THROTTLE_KEYS = (
    "SW Thermal Slowdown",
    "HW Thermal Slowdown",
    "HW Power Brake Slowdown",
)


def parse_nvidia_smi(report: str) -> List[str]:
    """The faults a policy can act on in ``nvidia-smi -q``: uncorrectable ECC
    errors since the driver loaded, remapped rows, throttling, and a GPU at its
    slowdown temperature."""
    faults = set()
    section = None
    in_remapped = False
    current_temp = slowdown_temp = None
    for line in report.splitlines():
        stripped = line.strip()
        if stripped in ("Volatile", "Aggregate"):
            section = stripped
            continue
        if stripped == "Remapped Rows":
            section, in_remapped = None, True
            continue
        if stripped.endswith("Errors") or stripped in ("Temperature", "Retired Pages"):
            section, in_remapped = None, False
        key, sep, value = stripped.partition(":")
        if not sep:
            continue
        key, value = key.strip(), value.strip()
        if section == "Volatile" and key in _UNCORRECTABLE_KEYS and _as_float(value):
            faults.add("ecc_uncorrectable")
        elif key in _THROTTLE_KEYS and value == "Active":
            faults.add("thermal_throttle" if "Thermal" in key else "power_brake")
        elif key == "GPU Current Temp":
            current_temp = _as_float(value.removesuffix("C"))
        elif key == "GPU Slowdown Temp":
            slowdown_temp = _as_float(value.removesuffix("C"))
        elif in_remapped and key == "Uncorrectable Error" and _as_float(value):
            faults.add("remapped_rows")
        elif in_remapped and key == "Remapping Failure Occurred" and value == "Yes":
            faults.add("row_remap_failure")
    if current_temp is not None and current_temp >= (slowdown_temp or 90.0):
        faults.add("gpu_hot")
    return sorted(faults)


# ----------------------------------------------------------------------
# Hang detection
# ----------------------------------------------------------------------
def describe_comms(report: RASReport, comm_ids: List[str]) -> str:
    """One line per communicator: its launch counts by communicator rank."""
    lines = []
    for comm_id in comm_ids:
        processes = report.comm_rank_processes.get(comm_id, {})
        ops = []
        for op, ranks_by_count in sorted(report.mismatched_op_ranks(comm_id).items()):
            counts = "; ".join(
                f"{count} by ranks {sorted(ranks)} "
                f"({', '.join(f'{processes[r][0]}:{processes[r][1]}' for r in sorted(ranks) if r in processes)})"
                for count, ranks in sorted(ranks_by_count.items(), reverse=True)
            )
            ops.append(f"{op} launches: {counts}")
        lines.append(f"  - communicator {comm_id}: {'. '.join(ops)}")
    return "\n".join(lines)


class LogRASReport(Evaluator):
    """Logs NCCL's text report of a suspected hang, and lets training go on.

    Args:
        on_done: Called once the report is evaluated.
    """

    def __init__(self, on_done: Callable[[], None]):
        self._on_done = on_done

    def evaluate(self, state: HealthState):
        self._on_done()
        for node, result in sorted(
            state.probe_results.get("NCCLRASTextProbe", {}).items()
        ):
            if result.error is None and result.detail:
                logger.warning(
                    "NCCL's ncclras report of the suspected hang, from node %s:\n%s",
                    node[:8],
                    result.detail.rstrip(),
                )
                break
        return Noop()


class HangDiagnostics(Evaluator):
    """Saves the diagnostics of a confirmed hang, then evicts the nodes whose
    GPUs show a fault, or reattempts (``action="fail"``) or only logs
    (``action="observe"``).

    Args:
        message: What the hang detector saw.
        history: The raw ``ncclras -f json`` reports before the hang.
        action: ``"fail"`` or ``"observe"``.
        artifacts_dir: Where to write the diagnostics, if anywhere.
        on_done: Called once the diagnostics are evaluated.
    """

    def __init__(
        self,
        message: str,
        history: List[str],
        action: str,
        artifacts_dir: Optional[str],
        on_done: Callable[[], None],
    ):
        self._message = message
        self._history = history
        self._action = action
        self._artifacts_dir = artifacts_dir
        self._on_done = on_done

    def evaluate(self, state: HealthState):
        self._on_done()
        results = state.probe_results
        if self._artifacts_dir:
            files = {
                f"nccl_ras/history_{i:03d}.json": r for i, r in enumerate(self._history)
            }
            for name, by_key, suffix in [
                ("flight_recorder", results.get("FlightRecorderProbe", {}), "json"),
                ("stack_traces", results.get("StackTraceProbe", {}), "log"),
                ("nvidia_smi", results.get("NvidiaSmiProbe", {}), "log"),
                ("nccl_ras", results.get("NCCLRASTextProbe", {}), "txt"),
            ]:
                for key, result in by_key.items():
                    body = (
                        result.detail
                        if result.error is None
                        else f"error: {result.error}"
                    )
                    files[f"{name}/{str(key)[:16]}.{suffix}"] = body or ""
            for rel, body in files.items():
                path = os.path.join(self._artifacts_dir, rel)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as f:
                    f.write(body)

        faulty = {
            node: parse_nvidia_smi(result.detail)
            for node, result in results.get("NvidiaSmiProbe", {}).items()
            if result.error is None and result.detail
        }
        faulty = {node: faults for node, faults in faulty.items() if faults}
        where = f" Diagnostics: {self._artifacts_dir}." if self._artifacts_dir else ""
        if faulty:
            return Evict(
                reason=f"{self._message} GPU faults: {faulty}.{where}",
                target_nodes=sorted(faulty),
            )
        if self._action == "fail":
            return Reattempt(reason=f"{self._message} No GPU fault found.{where}")
        logger.warning("%s No GPU fault found.%s", self._message, where)
        return Noop()


class NCCLHangEvaluator(Evaluator):
    """The merged NCCL RAS hang detector's rule, as an evaluator.

    A communicator is frozen when it's mismatched (its ranks launched different
    numbers of an op) and no rank advanced any op since the previous report. A
    frozen streak of ``confirm_duration_s`` confirms the hang. Each ``Diagnose``
    repeats until its diagnosis runs, since one returned while other on-demand
    checks run is dropped.

    Args:
        poll_interval_s: How often ``NCCLRASProbe`` is sampled.
        confirm_duration_s: How long a communicator stays frozen to confirm.
        action: ``"fail"`` reattempts a confirmed hang, ``"observe"`` only logs.
        artifacts_dir: Where ``HangDiagnostics`` writes the diagnostics.
    """

    def __init__(
        self,
        poll_interval_s: float,
        confirm_duration_s: float,
        action: str = "fail",
        artifacts_dir: Optional[str] = None,
    ):
        self._poll_interval_s = poll_interval_s
        self._confirm_polls = math.ceil(confirm_duration_s / poll_interval_s)
        self._suspicion_polls = min(
            math.ceil(_FIRST_SUSPICION_AFTER_S / poll_interval_s),
            self._confirm_polls - 1,
        )
        self._action = action
        self._artifacts_dir = artifacts_dir
        self.on_reset()

    def on_reset(self) -> None:
        self._disabled = False
        self._last_timestamp = None
        self._prev: Optional[RASReport] = None
        self._streaks: Dict[str, int] = {}
        self._clear_diagnoses()
        self._history: Deque[str] = deque(
            maxlen=self._confirm_polls + _HISTORY_MARGIN_POLLS
        )

    def _clear_diagnoses(self) -> None:
        self._ras_report_logged = False
        self._diagnosed = False

    def _mark_ras_report_logged(self) -> None:
        self._ras_report_logged = True

    def _mark_diagnosed(self) -> None:
        self._diagnosed = True

    def evaluate(self, state: HealthState):
        if self._disabled:
            return Noop()
        results = state.probe_results.get("NCCLRASProbe", {})
        errors = [r.error for r in results.values() if r.error is not None]
        if (
            results
            and len(errors) == len(results)
            and all(getattr(e, "fatal", False) for e in errors)
        ):
            logger.warning("Disabling the NCCL hang detector: %s", errors[0])
            self._disabled = True
            return Noop()

        # Every node sees the whole RAS mesh: use the first that answered.
        fresh = [r for _, r in sorted(results.items()) if r.error is None and r.detail]
        if not fresh or fresh[0].timestamp_s == self._last_timestamp:
            return Noop()
        result = fresh[0]
        self._last_timestamp = result.timestamp_s
        report = parse_ras_schema(result.detail)
        if report is None:
            return Noop()
        self._history.append(report.raw_json)

        prev, self._prev = self._prev, report
        if prev is None or not report.mismatched_comms:
            self._streaks = {}
            self._clear_diagnoses()
            return Noop()
        diff = compute_report_op_diff(prev, report)
        self._streaks = {
            comm: self._streaks.get(comm, 0) + 1
            for comm in report.mismatched_comms
            if comm in diff
            and all(d == 0 for ops in diff[comm].values() for d in ops.values())
        }
        longest = max(self._streaks.values(), default=0)
        stalled = sorted(self._streaks)

        if longest >= self._confirm_polls and not self._diagnosed:
            message = (
                f"NCCL hang: {len(stalled)} communicator(s) made no collective "
                f"progress for {longest * self._poll_interval_s:.0f}s while their "
                f"ranks disagree on how many collectives they launched.\n"
                f"{describe_comms(report, stalled)}\n"
            )
            history = list(self._history)
            action, artifacts_dir = self._action, self._artifacts_dir
            return Diagnose(
                reason=message,
                checks=[
                    HealthCheck(
                        probe_creator=lambda: [
                            FlightRecorderProbe(),
                            StackTraceProbe(),
                            NvidiaSmiProbe(),
                            NCCLRASTextProbe(),
                        ],
                        evaluator_creator=lambda: [
                            HangDiagnostics(
                                message,
                                history,
                                action,
                                artifacts_dir,
                                self._mark_diagnosed,
                            )
                        ],
                    )
                ],
            )
        if longest == self._suspicion_polls:
            logger.warning(
                "Possible NCCL hang: %d communicator(s) made no progress for %.0fs.\n%s",
                len(stalled),
                longest * self._poll_interval_s,
                describe_comms(report, stalled),
            )
        if longest >= self._suspicion_polls and not (
            self._ras_report_logged or self._diagnosed
        ):
            return Diagnose(
                reason="possible NCCL hang",
                checks=[
                    HealthCheck(
                        probe_creator=lambda: [NCCLRASTextProbe()],
                        evaluator_creator=lambda: [
                            LogRASReport(self._mark_ras_report_logged)
                        ],
                    )
                ],
            )
        return Noop()


def nccl_hang_check(
    poll_interval_s: float = 15.0,
    confirm_duration_s: float = 600.0,
    action: str = "fail",
    artifacts_dir: Optional[str] = None,
) -> PeriodicHealthCheck:
    """The in-flight NCCL hang check, with the merged detector's defaults."""
    return PeriodicHealthCheck(
        probe_creator=lambda: [NCCLRASProbe()],
        evaluator_creator=lambda: [
            NCCLHangEvaluator(
                poll_interval_s, confirm_duration_s, action, artifacts_dir
            )
        ],
        poll_interval_s=poll_interval_s,
    )
