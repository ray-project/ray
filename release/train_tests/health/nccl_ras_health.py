"""NCCL RAS hang detection, written as a user of ``ray.train.health``.

Not part of Ray Train: it imports only ``ray.train.health``, which is the test
that the public contracts are enough to bring your own probe.

    import ray.train.health as health
    from nccl_ras_health import nccl_ras_policy

    trainer = TorchTrainer(
        train_func,
        run_config=RunConfig(
            health_config=health.HealthConfig(policies=[nccl_ras_policy()]),
        ),
    )

- ``NcclRasProbe``: a ``ClusterProbe`` keyed by communicator.
- ``NcclRasReadyProbe``: pre-flight check that a node can run RAS at all.
- ``NcclHangEvaluator``: the merged detector's rule (#64928).
- ``CollectiveHangEvaluator``: the same signal joined with ``health.report()``.
- Diagnostics from #66229: ``StackTraceProbe``, ``NvidiaSmiProbe``,
  ``RasTextReportProbe``.

Scripts using this module call
``ray.cloudpickle.register_pickle_by_value(nccl_ras_health)``, because it is
not importable on the controller or the workers.
"""
import json
import logging
import os
import re
import statistics
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Tuple

import ray
import ray.train.health as health
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

_RAS_QUERY_TIMEOUT_S = 8.0


# ======================================================================
# Parsing `ncclras -f json`
# ======================================================================
@dataclass
class RasComm:
    """One communicator, with ranks already translated to *global* ranks."""

    comm_id: str
    op_counts: Dict[int, Dict[str, int]] = field(default_factory=dict)
    running: Dict[int, bool] = field(default_factory=dict)
    # False when keys are the communicator's own rank numbering.
    ranks_are_global: bool = True

    @property
    def mismatched(self) -> bool:
        """Op counts differ across ranks, and every rank is still RUNNING."""
        if not self.op_counts or not all(self.running.values()):
            return False
        ops = {op for counts in self.op_counts.values() for op in counts}
        return any(
            len({counts.get(op, 0) for counts in self.op_counts.values()}) > 1
            for op in ops
        )


# NCCL 2.28.9 emits `missing_ranks[]` with a missing comma between entries.
_MISSING_COMMA_RE = re.compile(r'([\d"el])(\s*\n\s*)("[^"\n]*"\s*:)')


def parse_ras_json(text: str) -> Optional[Dict[str, RasComm]]:
    """Parse ``ncclras -f json`` into communicators keyed by hash.

    RAS numbers ranks *within each communicator*: a two-rank subgroup reports
    ranks 0 and 1 whichever global ranks it spans. Found on a real 4-rank run
    where four different subgroups all reported ``{0, 1}``. The process is the
    only identity comparable across communicators, so ranks are translated
    through ``(host, pid)`` against the widest communicator -- the world group
    every parallelism strategy creates, the only one whose local ranks are the
    global ones.

    Returns:
        ``{comm_hash: RasComm}``, or ``None`` if the output does not parse.
    """
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        try:
            data = json.loads(_MISSING_COMMA_RE.sub(r"\1,\2\3", text))
        except (json.JSONDecodeError, TypeError):
            return None

    try:
        comms = data["communicators"]
        proc_of = {
            comm["hash"]: {
                r["rank"]: (r.get("host"), r.get("pid")) for r in comm["ranks"]
            }
            for comm in comms
        }
        world = max(proc_of.values(), key=len) if proc_of else {}
        to_global = {proc: rank for rank, proc in world.items() if None not in proc}

        out: Dict[str, RasComm] = {}
        for comm in comms:
            procs = proc_of[comm["hash"]]
            translatable = all(procs[r["rank"]] in to_global for r in comm["ranks"])
            parsed = RasComm(comm_id=comm["hash"], ranks_are_global=translatable)
            for r in comm["ranks"]:
                rank = to_global[procs[r["rank"]]] if translatable else r["rank"]
                parsed.op_counts[rank] = {
                    op: int(n) for op, n in r["collective_counts"].items()
                }
                status = r["status"]
                parsed.running[rank] = (
                    not status.get("abort_flag")
                    and not status.get("finalize_called")
                    and not status.get("destroy_flag")
                    and status.get("init_state", 0) == 0
                )
            out[comm["hash"]] = parsed
        return out
    except (KeyError, TypeError, ValueError):
        return None


# ======================================================================
# Collect: the probe
# ======================================================================
@ray.remote(num_cpus=0)
def _run_on_node(cmd: str, timeout_s: float) -> Tuple[int, str, str]:
    try:
        p = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout_s
        )
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return 124, "", f"timed out after {timeout_s:.0f}s"


class NcclRasProbe(health.ClusterProbe):
    """One ``ncclras`` query per poll, reduced to one result per communicator.

    RAS listens on localhost inside each rank's process, so the query has to
    run on a node hosting a rank -- it is dispatched as a pinned task and tried
    node by node until one answers. One answer covers the whole mesh.

    Holds the previous snapshot so it can emit a delta. That is safe only
    because a cluster probe runs on the controller, outside the failure domain
    of what it measures.
    """

    name = "NcclRasProbe"
    entity = "communicator"

    def __init__(self, interval_s: float = 15.0, binary: str = "ncclras"):
        self.interval_s = interval_s
        self._binary = binary
        self._prev: Optional[Dict[str, RasComm]] = None

    def query(self, ctx: health.ClusterContext) -> Optional[str]:
        """Raw ``ncclras -f json`` from the first node that answers."""
        nodes = list(dict.fromkeys(ctx.rank_to_node.values())) or ctx.node_ids
        cmd = f"{self._binary} -f json -t {int(_RAS_QUERY_TIMEOUT_S - 3)}"
        for node_id in nodes:
            try:
                rc, out, err = ray.get(
                    _run_on_node.options(
                        scheduling_strategy=NodeAffinitySchedulingStrategy(
                            node_id=node_id, soft=False
                        )
                    ).remote(cmd, _RAS_QUERY_TIMEOUT_S),
                    timeout=_RAS_QUERY_TIMEOUT_S + 5,
                )
            except Exception:  # noqa: BLE001
                continue
            if "invalid option -- 'f'" in err:
                raise RuntimeError(f"{self._binary} is too old for `-f json`")
            if rc == 127:
                raise RuntimeError(f"{self._binary} not found on {node_id}")
            if rc == 0 and out.strip():
                return out
            # "Connection refused" until the communicators exist; try the next.
        return None

    def poll(self, ctx: health.ClusterContext) -> Dict[str, health.ProbeResult]:
        raw = self.query(ctx)
        comms = parse_ras_json(raw) if raw else None
        if comms is None:
            return {}
        prev, self._prev = self._prev, comms
        return {cid: self._reduce(c, (prev or {}).get(cid)) for cid, c in comms.items()}

    @staticmethod
    def _reduce(comm: RasComm, prev: Optional[RasComm]) -> health.ProbeResult:
        # No comparable previous sample (first poll, new communicator) is not
        # the same as no progress.
        comparable = prev is not None and set(prev.op_counts) == set(comm.op_counts)
        advanced = {
            rank: comparable and counts != prev.op_counts.get(rank)
            for rank, counts in comm.op_counts.items()
        }
        any_advanced = any(advanced.values())
        frozen = comparable and comm.mismatched and not any_advanced
        return health.ProbeResult(
            metrics={
                "mismatched": float(comm.mismatched),
                "ops_advanced": float(any_advanced),
                "ranks": float(len(comm.op_counts)),
                "ranks_are_global": float(comm.ranks_are_global),
            },
            devices={
                str(rank): {
                    "op_count": float(sum(comm.op_counts[rank].values())),
                    "ops_advanced": float(advanced[rank]),
                }
                for rank in comm.op_counts
            },
            events=["frozen"] if frozen else [],
            passed=(not frozen) if comparable else None,
            detail=(
                f"communicator {comm.comm_id}: ranks {sorted(comm.op_counts)} "
                "mismatched and making no progress"
                if frozen
                else ""
            ),
        )


# ======================================================================
# Act: diagnostics (from #66229), as on-demand probes
# ======================================================================
def _upload(ctx: health.OnDemandProbeContext, tool: str, name: str, body: str):
    if ctx.upload is None:
        return []
    try:
        return [ctx.upload(tool, {name: body})]
    except Exception:  # noqa: BLE001
        logger.exception("Failed to upload %s output for %s.", tool, ctx.entity_id)
        return []


class StackTraceProbe(health.OnDemandProbe):
    """Native + Python stacks of one training process.

    Worker-scoped: ``py-spy`` has to attach to the training process, and only
    the worker knows which process that is. Falls back to a Python-only
    traceback where ptrace is blocked -- which loses the native frames a NCCL
    hang sits in, and says so in the output.
    """

    name = "StackTraceProbe"
    scope = health.WORKER_SCOPE
    timeout_s = 30.0

    def poll(self, ctx: health.OnDemandProbeContext) -> health.ProbeResult:
        pid = os.getpid()
        try:
            p = subprocess.run(
                ["py-spy", "dump", "--pid", str(pid), "--native"],
                capture_output=True,
                text=True,
                timeout=max(ctx.timeout_s - 5, 1),
            )
            trace = p.stdout if p.returncode == 0 and p.stdout.strip() else None
            why = (p.stderr or "").strip() or f"py-spy exited {p.returncode}"
        except FileNotFoundError:
            trace, why = None, "py-spy not installed"
        except subprocess.TimeoutExpired:
            trace, why = None, "py-spy timed out"

        native = trace is not None
        if not native:
            import sys
            import traceback

            lines = [f"[py-spy unavailable: {why}; Python-only traceback follows]"]
            for tid, frame in sys._current_frames().items():
                stack = "".join(traceback.format_stack(frame))
                lines.append(f"\n# Thread {tid}\n{stack}")
            trace = "\n".join(lines)

        return health.ProbeResult(
            metrics={"native": float(native)},
            detail=f"rank {ctx.rank}: {'native' if native else 'python-only'} stack",
            artifacts=_upload(ctx, "stack_traces", f"rank_{ctx.rank}.log", trace),
        )


def _as_float(text: str) -> Optional[float]:
    try:
        return float(text.strip())
    except (TypeError, ValueError):
        return None


# Field names taken from real `nvidia-smi -q` output on an A10G.
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


def parse_nvidia_smi(report: str) -> Tuple[Dict[str, float], List[str]]:
    """The few counters a policy can act on, from `nvidia-smi -q`.

    ``Volatile`` ECC counts since the last driver reload (this run);
    ``Aggregate`` is the card's lifetime. Only the former says anything about
    the run in progress -- summing both makes every used GPU look faulty -- and
    only indentation separates them, so the parser tracks the section.
    """
    metrics: Dict[str, float] = {}
    events: List[str] = []
    ecc = {"Volatile": 0.0, "Aggregate": 0.0}
    section: Optional[str] = None
    in_remapped = False
    current_temp = slowdown_temp = None

    for line in report.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped in ecc:
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

        if section in ecc and key in _UNCORRECTABLE_KEYS:
            ecc[section] += _as_float(value) or 0.0
        elif key in _THROTTLE_KEYS and value == "Active":
            # Match the value: the `Counters` section reuses these keys.
            events.append("thermal_throttle" if "Thermal" in key else "power_brake")
        elif key == "GPU Current Temp":
            current_temp = _as_float(value.removesuffix("C"))
        elif key == "GPU Slowdown Temp":
            slowdown_temp = _as_float(value.removesuffix("C"))
        elif in_remapped and key == "Uncorrectable Error" and _as_float(value):
            metrics["remapped_rows_uncorrectable"] = _as_float(value)
            events.append("remapped_rows")
        elif in_remapped and key == "Remapping Failure Occurred" and value == "Yes":
            events.append("row_remap_failure")

    metrics["ecc_uncorrectable"] = ecc["Volatile"]
    metrics["ecc_uncorrectable_lifetime"] = ecc["Aggregate"]
    if ecc["Volatile"]:
        events.append("ecc_uncorrectable")
    if current_temp is not None:
        limit = slowdown_temp or 90.0
        metrics["max_temp_c"] = current_temp
        metrics["slowdown_temp_c"] = limit
        if current_temp >= limit:
            events.append("gpu_hot")
    return metrics, sorted(set(events))


class NvidiaSmiProbe(health.OnDemandProbe):
    """Every GPU on one host, at the moment of the hang.

    Node-scoped: the GPUs belong to the host, and the check must not depend on
    a training process that may have stopped answering. A driver that will not
    answer inside the timeout is itself the finding.
    """

    name = "NvidiaSmiProbe"
    scope = health.NODE_SCOPE
    timeout_s = 30.0

    def poll(self, ctx: health.OnDemandProbeContext) -> health.ProbeResult:
        try:
            p = subprocess.run(
                ["nvidia-smi", "-q"],
                capture_output=True,
                text=True,
                timeout=max(ctx.timeout_s - 5, 1),
            )
        except FileNotFoundError:
            return health.ProbeResult(detail="no nvidia-smi on this node")
        except subprocess.TimeoutExpired:
            return health.ProbeResult(
                metrics={"driver_responded": 0.0},
                events=["nvidia_smi_unresponsive"],
                passed=False,
                detail=f"node {ctx.node_id}: nvidia-smi hung; the driver is stuck",
            )
        if p.returncode != 0:
            return health.ProbeResult(
                passed=False, detail=f"nvidia-smi exited {p.returncode}"
            )

        metrics, events = parse_nvidia_smi(p.stdout)
        return health.ProbeResult(
            metrics={"driver_responded": 1.0, **metrics},
            events=events,
            passed=not events,
            detail=f"node {ctx.node_id}: " + (", ".join(events) or "GPUs clean"),
            artifacts=_upload(ctx, "nvidia_smi", f"{ctx.node_id}.log", p.stdout),
        )


class RasTextReportProbe(health.OnDemandProbe):
    """``ncclras -f text`` -- the human-readable report, which names the culprit.

    Node-scoped so it reaches RAS on localhost without going through a training
    process.
    """

    name = "RasTextReportProbe"
    scope = health.NODE_SCOPE
    timeout_s = 15.0

    def poll(self, ctx: health.OnDemandProbeContext) -> health.ProbeResult:
        p = subprocess.run(
            "ncclras -f text -t 5", shell=True, capture_output=True, text=True
        )
        if p.returncode != 0 or not p.stdout.strip():
            return health.ProbeResult(detail="no RAS text report on this node")
        return health.ProbeResult(
            detail="RAS text report captured",
            artifacts=_upload(ctx, "nccl_ras", f"{ctx.node_id}.txt", p.stdout),
        )


def hang_diagnostics() -> List[health.OnDemandProbe]:
    """The checks #64928 and #66229 run on a confirmed hang."""
    return [StackTraceProbe(), NvidiaSmiProbe(), RasTextReportProbe()]


# ======================================================================
# Pre-flight
# ======================================================================
_MIN_NCCL = (2, 28)


def _major_minor(version) -> Optional[Tuple[int, int]]:
    if isinstance(version, int):  # older torch: 22809 for 2.28.9
        return version // 10000, version // 100 % 100
    if isinstance(version, tuple):
        return tuple(version[:2])
    m = re.search(r"(\d+)\.(\d+)", version or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _pyspy_can_attach() -> bool:
    """Whether py-spy can attach to a Python process it did not start."""
    import sys

    target = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        time.sleep(1)
        p = subprocess.run(
            ["py-spy", "dump", "--pid", str(target.pid)],
            capture_output=True,
            timeout=20,
        )
        return p.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    finally:
        target.kill()


class NcclRasReadyProbe(health.OnDemandProbe):
    """Can this node run NCCL RAS hang detection?

    Fails the node if the ``ncclras`` client or the in-process NCCL is older
    than 2.28, the first with ``ncclras -f json``. Reports, but does not fail
    on, whether ``py-spy`` can capture native stacks.
    """

    scope = health.NODE_SCOPE
    timeout_s = 60.0

    def poll(self, ctx: health.OnDemandProbeContext) -> health.ProbeResult:
        problems = []
        p = subprocess.run(
            "ncclras --version 2>&1", shell=True, capture_output=True, text=True
        )
        client = _major_minor(p.stdout) if p.returncode == 0 else None
        if client is None:
            problems.append("ncclras not found")
        elif client < _MIN_NCCL:
            problems.append(f"ncclras {client[0]}.{client[1]} is older than 2.28")

        try:
            import torch

            lib = _major_minor(torch.cuda.nccl.version())
        except Exception as e:  # noqa: BLE001
            lib = None
            problems.append(f"cannot read the in-process NCCL version: {e}")
        if lib is not None and lib < _MIN_NCCL:
            problems.append(f"in-process NCCL {lib[0]}.{lib[1]} is older than 2.28")

        native = _pyspy_can_attach()
        return health.ProbeResult(
            metrics={"pyspy_native": float(native)},
            passed=not problems,
            detail="; ".join(problems)
            or "ready" + ("" if native else " (py-spy: Python-only stacks)"),
        )


# ======================================================================
# Decide: the evaluators
# ======================================================================
def _faulty_hosts(state: health.HealthState) -> Dict[str, str]:
    """Nodes whose on-demand GPU snapshot came back bad."""
    return {
        node_id: ", ".join(r.events)
        for node_id, r in state.on_demand_probe_results(NvidiaSmiProbe).items()
        if r.passed is False and r.events
    }


def _ranks_of(result: health.ProbeResult) -> List[int]:
    return sorted(int(r) for r in result.devices)


class NcclHangEvaluator(health.Evaluator):
    """The merged detector's rule, then attribution.

    A mismatched communicator that makes no progress for ``confirm_duration_s``
    is confirmed hung. On confirmation it asks for diagnostics once, at the
    stalled ranks and their hosts; when they come back it decides:
    ``Evict(HARDWARE)`` if a host's GPUs are bad, else ``Reattempt(NO_PROGRESS)``.
    A hang alone never evicts -- that would let a bug in user code take out a
    healthy node.
    """

    def __init__(
        self,
        confirm_duration_s: float = 600.0,
        diagnostics: Optional[List[health.OnDemandProbe]] = None,
        clock=time.monotonic,
    ):
        self._confirm_s = confirm_duration_s
        self._diagnostics = list(diagnostics or [])
        self._clock = clock
        self._frozen_since: Dict[str, float] = {}
        self._diagnosed: set = set()

    def on_worker_group_start(self) -> None:
        self._frozen_since.clear()
        self._diagnosed.clear()

    def evaluate(self, state: health.HealthState) -> List[health.HealthDecision]:
        comms = state.results(NcclRasProbe)
        now = self._clock()
        frozen = {c for c, r in comms.items() if "frozen" in r.events}
        for comm_id in list(self._frozen_since):
            if comm_id not in frozen:
                self._frozen_since.pop(comm_id)
                self._diagnosed.discard(comm_id)

        confirmed = sorted(
            c
            for c in frozen
            if now - self._frozen_since.setdefault(c, now) >= self._confirm_s
        )
        if not confirmed:
            return []

        ranks = sorted({r for c in confirmed for r in _ranks_of(comms[c])})
        reason = (
            f"{len(confirmed)} of {len(comms)} NCCL communicators "
            f"({', '.join(confirmed)}) made no progress for {self._confirm_s:.0f}s"
        )
        return [_attribute(self, state, confirmed, ranks, reason)]


def _attribute(evaluator, state, confirmed, ranks, reason) -> health.HealthDecision:
    """Diagnose once, then decide on the evidence. Shared by both evaluators."""
    faulty = _faulty_hosts(state)
    if faulty:
        return health.Evict(
            cause=health.Cause.HARDWARE,
            reason=f"{reason}; diagnostics found "
            + "; ".join(f"{n} {w}" for n, w in sorted(faulty.items())),
            target_nodes=sorted(faulty),
        )

    undiagnosed = [c for c in confirmed if c not in evaluator._diagnosed]
    if evaluator._diagnostics and undiagnosed:
        evaluator._diagnosed.update(undiagnosed)
        return health.Diagnose(
            cause=health.Cause.NO_PROGRESS,
            reason=f"{reason}; running diagnostics before attributing it",
            on_demand_probes=list(evaluator._diagnostics),
            target_ranks=ranks,
            target_nodes=sorted({n for n in map(state.node_of, ranks) if n}),
        )

    if not evaluator._diagnostics:
        why = "no diagnostics are configured, so nothing examined the hardware"
    elif not state.on_demand_probe_results(NvidiaSmiProbe):
        why = "diagnostics were requested but have not reported back"
    else:
        why = "diagnostics found no hardware fault, so this is software or data"
    return health.Reattempt(cause=health.Cause.NO_PROGRESS, reason=f"{reason}; {why}")


#: `health.report()` keys naming a rank's position on each parallelism axis.
DEFAULT_AXES = ("dp_rank", "tp_rank", "pp_rank", "ep_rank")


def parallelism_groups(
    reported: Dict[int, dict], axes=DEFAULT_AXES
) -> Dict[FrozenSet[int], str]:
    """``{rank_set: axis}`` for every group the reported coordinates imply.

    The group for axis A containing rank r is every rank agreeing with r on
    every *other* axis: with tp=2, pp=2 over 4 ranks, TP groups are {0,1},{2,3}
    and PP groups are {0,2},{1,3}.
    """
    present = [a for a in axes if any(a in m for m in reported.values())]
    coords = {
        r: tuple(m[a] for a in present)
        for r, m in reported.items()
        if present and all(a in m for a in present)
    }
    groups: Dict[FrozenSet[int], str] = {}
    for i, axis in enumerate(present):
        buckets: Dict[tuple, set] = defaultdict(set)
        for rank, c in coords.items():
            buckets[c[:i] + c[i + 1 :]].add(rank)
        for ranks in buckets.values():
            if len(ranks) > 1:
                groups.setdefault(frozenset(ranks), axis)
    return groups


class CollectiveHangEvaluator(health.Evaluator):
    """NCCL RAS joined against what the training loop reports.

    RAS says a communicator is mismatched and not advancing. That is also what
    a *legitimately slow step* looks like: one rank pauses before the
    collective -- a checkpoint save, a GC pause, a slow data shard -- and its
    peers wait inside the all-reduce until it arrives. Whether that is a hang
    depends on what a normal step takes for *this* job, which only the job
    knows. So:

    - **The threshold is in the job's own units**: ``stall_factor x median
      step time`` from ``health.report()``, clamped to
      ``[min_stall_s, max_stall_s]``. A 30s pause is a hang for a job whose
      steps take 0.2s and noise for one whose steps take 10s; a single fixed
      confirm window is wrong for one of them.
    - **The communicator gets a name.** Matching its rank set against the
      groups the reported coordinates imply says it is the TP or DP group, which
      is what a person reading the decision needs. Only done when the probe
      translated ranks to global ranks; otherwise the group is left unnamed.

    A job that reports nothing falls back to ``max_stall_s``.
    """

    def __init__(
        self,
        stall_factor: float = 5.0,
        min_stall_s: float = 30.0,
        max_stall_s: float = 900.0,
        axes=DEFAULT_AXES,
        diagnostics: Optional[List[health.OnDemandProbe]] = None,
        clock=time.monotonic,
    ):
        self._stall_factor = stall_factor
        self._min_stall_s = min_stall_s
        self._max_stall_s = max_stall_s
        self._axes = tuple(axes)
        self._diagnostics = list(diagnostics or [])
        self._clock = clock
        self._frozen_since: Dict[str, float] = {}
        self._diagnosed: set = set()

    def on_worker_group_start(self) -> None:
        self._frozen_since.clear()
        self._diagnosed.clear()

    def stall_threshold(self, state: health.HealthState) -> float:
        times = [
            float(m["step_time_s"])
            for m in state.reported.values()
            if isinstance(m.get("step_time_s"), (int, float))
        ]
        if not times:
            return self._max_stall_s
        derived = statistics.median(times) * self._stall_factor
        return max(self._min_stall_s, min(self._max_stall_s, derived))

    def evaluate(self, state: health.HealthState) -> List[health.HealthDecision]:
        comms = state.results(NcclRasProbe)
        now = self._clock()
        frozen = {c: r for c, r in comms.items() if "frozen" in r.events}
        for comm_id in list(self._frozen_since):
            if comm_id not in frozen:
                self._frozen_since.pop(comm_id)
                self._diagnosed.discard(comm_id)

        threshold = self.stall_threshold(state)
        groups = parallelism_groups(state.reported, self._axes)
        confirmed, named = [], []
        for comm_id, result in sorted(frozen.items()):
            held = now - self._frozen_since.setdefault(comm_id, now)
            if held < threshold:
                continue
            axis = (
                groups.get(frozenset(_ranks_of(result)))
                if result.metrics.get("ranks_are_global")
                else None
            )
            label = axis.replace("_rank", "").upper() if axis else "unnamed"
            confirmed.append(comm_id)
            named.append(
                f"{comm_id} ({label} group, ranks {_ranks_of(result)}) "
                f"for {held:.0f}s"
            )

        if not confirmed:
            return []
        ranks = sorted({r for c in confirmed for r in _ranks_of(comms[c])})
        reason = (
            f"NCCL communicator {', '.join(named)} mismatched with no progress, "
            f"past this job's stall threshold of {threshold:.0f}s "
            f"({self._stall_factor:g} x median step time)"
        )
        return [_attribute(self, state, confirmed, ranks, reason)]


# ======================================================================
# Policies
# ======================================================================
def _probes(interval_s: float) -> List[health.Probe]:
    return [NcclRasProbe(interval_s=interval_s), NcclRasReadyProbe()]


def nccl_ras_policy(
    *,
    confirm_duration_s: float = 600.0,
    interval_s: float = 15.0,
    diagnose: bool = True,
    preflight: bool = True,
) -> health.HealthPolicy:
    """The merged detector, expressed as a policy."""
    return health.HealthPolicy(
        probe_creator=lambda: _probes(interval_s),
        preflight=preflight,
        evaluator_creator=lambda: [
            NcclHangEvaluator(
                confirm_duration_s=confirm_duration_s,
                diagnostics=hang_diagnostics() if diagnose else None,
            )
        ],
    )


def collective_hang_policy(
    *,
    stall_factor: float = 5.0,
    min_stall_s: float = 30.0,
    interval_s: float = 15.0,
    diagnose: bool = True,
    preflight: bool = True,
) -> health.HealthPolicy:
    """NCCL RAS plus the job's own progress and parallelism layout."""
    return health.HealthPolicy(
        probe_creator=lambda: _probes(interval_s),
        preflight=preflight,
        evaluator_creator=lambda: [
            CollectiveHangEvaluator(
                stall_factor=stall_factor,
                min_stall_s=min_stall_s,
                diagnostics=hang_diagnostics() if diagnose else None,
            )
        ],
    )
