"""The hang detector's diagnostics, as on-demand probes.

ray-project/ray#64928 landed the detector; #66229 adds an ``nvidia-smi``
snapshot to it. Between them the callback has grown a hand-built push path:
``fan_out_to_workers`` dispatches, ``capture_diagnostic`` isolates failures,
``upload_diagnostics`` writes one folder per tool, and ``handle_confirmed_hangs``
orchestrates. That is the REP's ``Diagnose`` action, written out longhand for
one detector.

Expressed as ``OnDemandProbe``s, the same three checks compose with anything
else in the loop, and -- the part the callback cannot do -- their results come
back as evidence the *next* Decide step reads, instead of files a human opens
after the run has already failed.

One correction the port makes: ``nvidia-smi`` is node-scoped. #66229 runs it
through one worker per node (dedup by ``node_ip``) because a worker is the only
remote hand available. But a wedged driver is exactly what it is looking for,
and a worker sitting in a stuck CUDA call may never answer -- so the snapshot
goes missing precisely when it matters. Scoped to the node, it runs in the
``NodeMonitor``, outside the training process.
"""
import logging
import subprocess
from typing import Any, Dict, Optional

from ray.train.v2._internal.execution.health.probe import (
    NODE_SCOPE,
    WORKER_SCOPE,
    OnDemandProbe,
    OnDemandProbeContext,
    ProbeResult,
)

logger = logging.getLogger(__name__)

_STACK_DUMP_TIMEOUT_S: float = 30.0
_NVIDIA_SMI_TIMEOUT_S: float = 30.0

# The worker-side capture from #64928, reused as-is.
from ray.train.v2._internal.callbacks.nccl_ras import dump_stack_trace  # noqa: E402

try:  # #66229, not yet merged
    from ray.train.v2._internal.callbacks.nccl_ras import run_nvidia_smi
except ImportError:

    def run_nvidia_smi(timeout_s: float) -> Dict[str, Any]:
        """Snapshot every GPU on this host.

        Mirrors #66229. ``nvidia-smi -q`` reports per GPU the driver and VBIOS
        versions, power draw, temperature, clocks and throttle reasons, ECC and
        retired-page counters, and the processes holding the device -- which is
        what lets a user decide whether a hang is the hardware rather than a
        divergent code path.

        The timeout is not optional: a wedged driver is the failure this is
        looking for, and ``nvidia-smi`` blocks inside the driver in that case.
        """
        try:
            proc = subprocess.run(
                ["nvidia-smi", "-q"],
                capture_output=True,
                text=True,
                timeout=timeout_s,
            )
        except FileNotFoundError:
            return {"ok": False, "reason": "binary_not_found"}
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "reason": (
                    f"`nvidia-smi -q` timed out after {timeout_s:.0f}s, which "
                    "usually means the driver is itself stuck"
                ),
            }
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "reason": f"error: {e}"}

        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            return {
                "ok": False,
                "reason": f"`nvidia-smi -q` exited {proc.returncode} ({stderr[:500]})",
            }
        return {"ok": True, "stdout": proc.stdout}


def _upload(ctx: OnDemandProbeContext, tool: str, filename: str, body: str) -> list:
    """Write one diagnostic's output to run storage, returning its path."""
    if ctx.upload is None:
        return []
    try:
        return [ctx.upload(tool, {filename: body})]
    except Exception:
        logger.exception("Failed to upload %s output for %s.", tool, ctx.entity_id)
        return []


class StackTraceProbe(OnDemandProbe):
    """Native + Python stacks of one training process.

    Worker-scoped because ``py-spy`` has to attach to the training process, and
    only the worker knows which process that is. Read-only: it does not need the
    accelerator, so training is never stopped for it.
    """

    name = "StackTraceProbe"
    scope = WORKER_SCOPE
    stop_workers = False
    timeout_s = _STACK_DUMP_TIMEOUT_S

    def poll(self, ctx: OnDemandProbeContext) -> ProbeResult:
        trace = dump_stack_trace(max(ctx.timeout_s - 5, 1))
        native = "py-spy unavailable" not in trace
        return ProbeResult(
            metrics={"native": float(native), "lines": float(trace.count("\n") + 1)},
            detail=f"rank {ctx.rank}: {'native' if native else 'python-only'} stack",
            artifacts=_upload(ctx, "stack_traces", f"rank_{ctx.rank}.log", trace),
        )


class NvidiaSmiProbe(OnDemandProbe):
    """Every GPU on one host, at the moment of the hang.

    Node-scoped: the GPUs belong to the host, not the rank, and the check has to
    survive a training process that has stopped answering.

    The parsed counters are what an evaluator acts on -- ECC errors and thermal
    throttling are hardware, and hardware is the only cause that authorizes
    quarantine. The full report goes to storage for the human.
    """

    name = "NvidiaSmiProbe"
    scope = NODE_SCOPE
    stop_workers = False
    timeout_s = _NVIDIA_SMI_TIMEOUT_S

    def poll(self, ctx: OnDemandProbeContext) -> ProbeResult:
        out = run_nvidia_smi(max(ctx.timeout_s - 5, 1))
        if not out["ok"]:
            reason = out["reason"]
            # A node with no driver is not a finding; a driver that would not
            # answer in time is the most important finding there is.
            absent = reason == "binary_not_found"
            return ProbeResult(
                metrics={"driver_responded": 0.0},
                events=[] if absent else ["nvidia_smi_unresponsive"],
                passed=None if absent else False,
                detail=f"no `nvidia-smi` snapshot: {reason}",
                artifacts=_upload(
                    ctx, "nvidia_smi", f"{ctx.node_id}.log", f"{reason}\n"
                ),
            )

        report = out["stdout"]
        metrics, events = _parse_nvidia_smi(report)
        return ProbeResult(
            metrics={"driver_responded": 1.0, **metrics},
            events=events,
            passed=not events,
            detail=(
                f"node {ctx.node_id}: "
                + (", ".join(events) if events else "GPUs clean")
            ),
            artifacts=_upload(ctx, "nvidia_smi", f"{ctx.node_id}.log", report),
        )


def _as_float(text: str):
    try:
        return float(text.strip())
    except (TypeError, ValueError):
        return None


#: Section headers under `ECC Errors`. "Volatile" counts since the last driver
#: reload, i.e. this run; "Aggregate" is the card's lifetime. Only the first
#: says anything about the run in progress -- summing both would make every
#: long-lived GPU look faulty.
_ECC_VOLATILE = "Volatile"
_ECC_AGGREGATE = "Aggregate"

#: Uncorrectable ECC counters, as `nvidia-smi -q` actually spells them. There
#: is no field called plain "Uncorrectable"; assuming there was is why this
#: check silently counted zero on every real GPU.
_UNCORRECTABLE_KEYS = (
    "SRAM Uncorrectable Parity",
    "SRAM Uncorrectable SEC-DED",
    "DRAM Uncorrectable",
)

#: Clock-throttle reasons that mean the card is being held back by its own
#: limits. `Clocks Event Reasons` reports Active/Not Active; the `Counters`
#: section repeats the same names with microsecond values, which is why the
#: value has to be matched too and not just the key.
_THROTTLE_KEYS = (
    "SW Thermal Slowdown",
    "HW Thermal Slowdown",
    "HW Power Brake Slowdown",
)

#: Row-remapping is how modern GPUs retire bad memory. A failure, or a pending
#: remap, is a card on its way out.
_REMAP_FAILURE_KEYS = ("Remapping Failure Occurred", "Pending")


def _parse_nvidia_smi(report: str) -> tuple:
    """Pull the few counters a policy can act on out of `nvidia-smi -q`.

    Deliberately shallow: the full report goes to storage, and only what an
    evaluator thresholds on crosses into the health loop.

    Written against real `nvidia-smi -q` output from an A10G rather than from
    memory -- the field names are not what you would guess, and getting them
    wrong fails silently in the worst direction, reporting a clean GPU.
    """
    metrics = {}
    events = []
    section = None  # the `ECC Errors` subsection we are inside
    in_remapped = False
    slowdown_temp = None
    current_temp = None
    ecc = {_ECC_VOLATILE: 0.0, _ECC_AGGREGATE: 0.0}

    for line in report.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        # Indentation is the only thing separating Volatile from Aggregate, so
        # track the section rather than matching keys globally.
        if stripped in (_ECC_VOLATILE, _ECC_AGGREGATE):
            section = stripped
            continue
        if stripped == "Remapped Rows":
            section, in_remapped = None, True
            continue
        if stripped.endswith("Errors") or stripped in ("Temperature", "Retired Pages"):
            section, in_remapped = None, stripped == "Remapped Rows"

        key, sep, value = stripped.partition(":")
        if not sep:
            continue
        key, value = key.strip(), value.strip()

        if section in ecc and key in _UNCORRECTABLE_KEYS:
            count = _as_float(value)
            if count is not None:
                ecc[section] += count
        elif key in _THROTTLE_KEYS and value == "Active":
            events.append("thermal_throttle" if "Thermal" in key else "power_brake")
        elif key == "GPU Current Temp":
            current_temp = _as_float(value.removesuffix("C"))
        elif key == "GPU Slowdown Temp":
            slowdown_temp = _as_float(value.removesuffix("C"))
        elif in_remapped and key == "Uncorrectable Error":
            count = _as_float(value)
            if count:
                metrics["remapped_rows_uncorrectable"] = count
                events.append("remapped_rows")
        elif in_remapped and key in _REMAP_FAILURE_KEYS and value == "Yes":
            events.append("row_remap_failure")
        elif key == "SRAM Threshold Exceeded" and value == "Yes":
            events.append("sram_threshold_exceeded")

    metrics["ecc_uncorrectable"] = ecc[_ECC_VOLATILE]
    metrics["ecc_uncorrectable_lifetime"] = ecc[_ECC_AGGREGATE]
    if ecc[_ECC_VOLATILE]:
        events.append("ecc_uncorrectable")

    if current_temp is not None:
        metrics["max_temp_c"] = current_temp
        # The threshold belongs to the card, not to us. An A10G slows at 95C, a
        # different part slows somewhere else, and a hardcoded number is wrong
        # for all but one of them.
        limit = slowdown_temp if slowdown_temp else 90.0
        metrics["slowdown_temp_c"] = limit
        if current_temp >= limit:
            events.append("gpu_hot")

    return metrics, sorted(set(events))


class RasTextReportProbe(OnDemandProbe):
    """``ncclras -f text``: the human-readable RAS report, at hang time.

    Worker-scoped and answered by any one rank -- the RAS mesh spans the job, so
    one reply describes all of it. It is what a person reads to see which ranks
    the collective is waiting on.
    """

    name = "RasTextReportProbe"
    scope = WORKER_SCOPE
    stop_workers = False
    timeout_s = 15.0

    def __init__(self, query_text):
        """
        Args:
            query_text: Callable returning the ``ncclras -f text`` output.
                Production wires this to ``RASPoller.query("text")``.
        """
        self._query_text = query_text

    def poll(self, ctx: OnDemandProbeContext) -> ProbeResult:
        report = self._query_text()
        if not report:
            return ProbeResult(passed=False, detail="no RAS text report available")
        return ProbeResult(
            metrics={"bytes": float(len(report))},
            detail="RAS text report captured",
            artifacts=_upload(ctx, "nccl_ras", f"rank_{ctx.rank}_ras.txt", report),
        )


def hang_diagnostics(query_text=None) -> list:
    """The three checks #64928 and #66229 run on a confirmed hang."""
    probes = [StackTraceProbe(), NvidiaSmiProbe()]
    if query_text is not None:
        probes.append(RasTextReportProbe(query_text))
    return probes
