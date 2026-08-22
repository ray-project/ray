"""Nvidia Hang Detector based on NCCL RAS + Diagnostics.

NCCL ships a Reliability/Availability/Serviceability (RAS) subsystem (NCCL
>= 2.24) that runs a monitoring thread inside *every* NCCL process (one per
GPU/rank). Those threads form a peer mesh tracking dead/ unresponsive ranks
and per-rank collective launch counts. Polling ``ncclras`` we can check whether
a communicator's collective counts between ranks are mismatched and if they
don't advance over sequential polls can indicate a hang.

Notes: At a confirmed hang (600 seconds), hang-diagnostics are written to:
    {experiment_directory}/nv_hang_detector_artifacts/
      nccl_ras/report.txt         human-readable ``-f text`` RAS report
      nccl_ras/{timestamp}.log    raw JSON RAS report per retained poll (timeline)
      stack_traces/{rank}.log     per-rank py-spy stack dump
      flight_recorder/{rank}.log  per-rank flight-recorder JSON (when armed)
      nvidia_smi/{node_ip}.log    per-node ``nvidia-smi -q``

Warning: RAS requires that all nodes are communicating, therefore, with no
"world" communicator with all ranks on it, RAS returns only a subset of the
ranks and communicators reachable from the queried node.

Warning: A known blind spot is symmetric in-collective hangs. RAS collective
counts are *launch* counts, not *completion* counts. If every rank launches
collective #N and the network/hardware then wedges mid-collective (a switch
failure or a NIC flap), every rank reports the same count N. The skew is zero,
no rank looks behind, and the detector will never flag it. Hardware health
metrics should be investigated in this case.
"""
import json
import logging
import os
import re
import subprocess
import tempfile
import time
from collections import defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeAlias,
    Union,
)

import ray
from ray._private.ray_constants import env_float, env_integer
from ray.exceptions import GetTimeoutError
from ray.train.v2._internal.constants import (
    DEFAULT_NCCL_RAS_ACTION,
    DEFAULT_NCCL_RAS_CONFIRM_WINDOW_S,
    DEFAULT_NCCL_RAS_POLL_INTERVAL_S,
    DEFAULT_NCCLRAS_BINARY_PATH,
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_ADDR_ENV_VAR,
    NCCL_RAS_CONFIRM_WINDOW_S_ENV_VAR,
    NCCL_RAS_POLL_INTERVAL_S_ENV_VAR,
    NCCLRAS_BINARY_PATH_ENV_VAR,
    TORCH_NCCL_TRACE_BUFFER_SIZE_ENV_VAR,
)
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.storage import _upload_to_fs_path
from ray.train.v2._internal.execution.worker_group import Worker, WorkerGroup
from ray.train.v2.api.exceptions import NCCLHangError

logger = logging.getLogger(__name__)

_NCCL_RAS_QUERY_TIMEOUT_S: float = 8.0  # the default ncclras -t value is 5
_DIAGNOSTIC_DUMP_TIMEOUT_S: float = 30.0  # timeout for py-spy, nvidia-smi, etc

# Limit the number of human-readable reports to prevent blocking the controller loop
_TEXT_QUERY_MAX_WORKERS: int = 3

# How many raw JSON RAS reports to retain for the postmortem timeline
_RAW_REPORT_HISTORY_SIZE: int = 20

# The directory within the experiment directory for artifacts to be saved
_HANG_ARTIFACT_DIR = "nv_hang_detector_artifacts"

# Number of consecutive polls where no worker produced usable data and stops querying
_MAX_CONSECUTIVE_QUERY_FAILURES: int = 20

# Escalation milestones, measured in seconds a communicator is frozen
_FIRST_SUSPICION_S: float = 60.0
_PERIODIC_WARN_EVERY_S: float = 120.0

# NCCL communicator is keyed by (hash, secondary_hash)
CommKey: TypeAlias = Tuple[str, str]


class NCCLRankHang(Exception):
    """A single worker's contribution to a confirmed NCCL hang.

    Used as the per-world-rank value in :attr:`NCCLHangError.worker_failures`
    so the culprit workers (stragglers / unresponsive ranks) are attributable
    and the error pickles cleanly back to the driver.
    """


def parse_ras_addr(addr: str) -> Tuple[str, int]:
    """Parse an ``NCCL_RAS_ADDR`` value (``host:port``) into ``(host, port)``.

    Handles ``host:port`` and bracketed IPv6 such as ``[::1]:28028``. The port
    is mandatory: a bare host is rejected rather than silently defaulted, so a
    misconfigured ``NCCL_RAS_ADDR`` surfaces instead of sending queries to a
    port the listener may not be on.

    Args:
        addr: The ``NCCL_RAS_ADDR`` value to parse.

    Returns:
        Tuple of ``(host, port)``

    Raises:
        ValueError: If ``addr`` is not ``host:port`` or ``[ipv6]:port``.
    """
    addr = addr.strip()
    if addr.startswith("["):  # [ipv6](:port)?
        end = addr.index("]")
        host, rest = addr[1:end], addr[end + 1 :]
        port = int(rest[1:])
        return host, port
    else:
        host, _, port = addr.rpartition(":")
        return host, int(port)


def run_ncclras(
    binary_path: str, timeout_s: float, fmt: str = "json"
) -> Dict[str, Any]:
    """Run ``ncclras`` on a worker and return its output.

    Args:
        binary_path: Path to the ``ncclras`` client binary.
        timeout_s: Per-query timeout passed to ``ncclras``.
        fmt: ``ncclras`` output format. ``"json"`` (machine-parsed by the
            poller) or ``"text"`` (``ncclras``'s human-readable report, logged
            at hang time).

    Returns:
        A dict ``{"ok": bool, ...}``. On success ``stdout`` holds the raw
        output. On failure ``reason`` distinguishes a missing binary (so the
        detector can degrade to a no-op) from transient errors.
    """
    host, port = parse_ras_addr(
        os.environ.get(NCCL_RAS_ADDR_ENV_VAR, "localhost:28028")
    )
    cmd = [
        binary_path,
        "-f",
        fmt,
        "-h",
        host,
        "-p",
        str(port),
        "-t",
        # ncclras takes an *integer* number of seconds; a float string such as
        # "5.0" is rejected by the 2.30.x client ("Invalid timeout: 5.0").
        str(int(timeout_s)),
    ]
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout_s + 5
        )
    except FileNotFoundError:
        return {"ok": False, "reason": "binary_not_found"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": f"timeout ({timeout_s} + 5)"}
    except Exception as e:
        return {"ok": False, "reason": f"error: {e}"}

    if proc.returncode != 0:
        stderr = proc.stderr or ""
        if "invalid option -- 'f'" in stderr:
            return {
                "ok": False,
                "reason": "unsupported_f_option",
                "stderr": stderr[:500],
            }
        return {
            "ok": False,
            "reason": f"exit_{proc.returncode}",
            "stderr": stderr[:500],
        }

    return {"ok": True, "stdout": proc.stdout}


@dataclass
class RASReport:
    """Structured summary of an ``ncclras`` JSON report.

    All rank keys are *communicator-local* ranks as reported by RAS (a rank's
    index within that communicator), NOT training world ranks. Detection only
    ever compares ranks within a single communicator, so local ranks suffice;
    the world-rank join for user-facing attribution happens in
    :meth:`NvHangDetectorCallback.identify_culprit_ranks` via each rank's
    ``(host, pid)``.

    Attributes:
        timestamp: When ``ncclras`` responded (RAS' own timestamp string). Used
            to skip stale reports: a wedged RAS agent that returns an identical
            snapshot would otherwise show zero deltas and count toward a hang.
        comm_collective_counts: Maps each communicator key to ``{local_rank:
            {collective_name: launch_count}}`` for ALL of that communicator's
            present ranks (no filtering, no majority logic).
        comm_rank_status: Maps each communicator to ``{local_rank: status}``
            (RUNNING / FINALIZE / INIT / ABORT). A frozen comm is only a hang
            when at least one rank is still RUNNING.
        comm_rank_info: Maps each communicator to ``{local_rank: {host, pid,
            cuda_dev}}`` -- kept so a confirmed hang can name the culprit ranks.
        comm_missing_ranks: Maps each communicator to ``{local_rank:
            {unresponsive, considered_dead, host, pid}}`` for ranks RAS knows
            about but could not reach. An ``unresponsive`` rank is a hang signal
            even when the surviving ranks show equal (zero-skew) counts.
    """

    timestamp: str
    comm_collective_counts: Dict[CommKey, Dict[int, Dict[str, int]]]
    comm_rank_status: Dict[CommKey, Dict[int, str]]
    comm_rank_info: Dict[CommKey, Dict[int, Dict[str, Any]]] = field(
        default_factory=dict
    )
    comm_missing_ranks: Dict[CommKey, Dict[int, Dict[str, Any]]] = field(
        default_factory=dict
    )

    @property
    def comm_collective_skews(self) -> Dict[CommKey, Dict[str, int]]:
        """Per-communicator, per-collective spread (max-min) across ranks, this poll.

        Returns:
            ``{comm_key: {collective_name: max_count - min_count for ranks}}``.
        """
        skews: Dict[CommKey, Dict[str, int]] = {}
        for comm, ranks in self.comm_collective_counts.items():
            per_collective = defaultdict(list)
            for counts in ranks.values():
                for name, count in counts.items():
                    per_collective[name].append(count)
            skews[comm] = {
                name: max(counts) - min(counts)
                for name, counts in per_collective.items()
            }
        return skews

    @property
    def mismatched_comms(self) -> Set[CommKey]:
        """Communicators whose ranks disagree on a collective count.

        A communicator is mismatched when some collective's count is skewed
        across its ranks AND at least one rank is still RUNNING. Requiring only
        *one* RUNNING rank (rather than all) is deliberate: the classic
        "a rank exited early" hang is a mix of FINALIZE rank(s) and frozen
        RUNNING peers, which is a stronger hang signal, not a weaker one.
        """
        result: Set[CommKey] = set()
        for comm, skews in self.comm_collective_skews.items():
            if not any(spread > 0 for spread in skews.values()):
                continue
            statuses = self.comm_rank_status.get(comm, {})
            if any(status == "RUNNING" for status in statuses.values()):
                result.add(comm)
        return result

    @property
    def unresponsive_comms(self) -> Set[CommKey]:
        """Communicators with at least one rank RAS could not reach."""
        return {
            comm
            for comm, ranks in self.comm_missing_ranks.items()
            if any(info.get("unresponsive") for info in ranks.values())
        }

    @property
    def healthy(self) -> bool:
        """True iff no communicator is mismatched and none has an unresponsive rank."""
        return not self.mismatched_comms and not self.unresponsive_comms

    @staticmethod
    def rank_status(status: Dict[str, Any]) -> str:
        """Convert a rank's status dict to a human-readable string."""
        # TODO: `async_error` is ignored currently
        if status["abort_flag"] is True:
            return "ABORT"
        if status["finalize_called"] is True or status["destroy_flag"] is True:
            return "FINALIZE"
        if status["init_state"] == 0:
            return "RUNNING"
        else:
            return "INIT"


def compute_report_collective_diff(
    prev: RASReport, curr: RASReport
) -> Dict[CommKey, Dict[int, Dict[str, int]]]:
    """Per-communicator, per-rank collective-count delta between two RAS reports.

    Args:
        prev: The previous poll's report.
        curr: The current poll's report.

    Returns:
        ``{comm_key: {local_rank: {collective_name: cur - prev}}}``, restricted
        to the communicators and ranks the two reports share.
    """
    collective_diff: Dict[CommKey, Dict[int, Dict[str, int]]] = {}
    for comm, comm_curr_counts in curr.comm_collective_counts.items():
        comm_prev_counts = prev.comm_collective_counts.get(comm)
        if not comm_prev_counts:
            continue

        comm_diff: Dict[int, Dict[str, int]] = {}
        for rank, curr_counts in comm_curr_counts.items():
            if rank not in comm_prev_counts:
                continue

            prev_counts = comm_prev_counts[rank]
            comm_diff[rank] = {
                name: count - prev_counts.get(name, 0)
                for name, count in curr_counts.items()
            }

        collective_diff[comm] = comm_diff
    return collective_diff


def format_comm(comm: CommKey) -> str:
    """Render a communicator key for logs/messages as ``hash/secondary_hash``."""
    return f"{comm[0]}/{comm[1]}"


# NCCL 2.28.9 emits malformed JSON for a ``missing_ranks[]`` with a missing comma
_MISSING_COMMA_RE = re.compile(r'([\d"el])(\s*\n\s*)("[^"\n]*"\s*:)')


def parse_ras_schema(ras_json: str) -> Optional[RASReport]:
    """Parse ``ncclras -f json`` output into a :class:`RASReport`.

    Targets the NCCL 2.28-2.30 JSON schema::

        {
          "nccl_version": ..., "communicators_count": N,
          "communicators": [
            {
              "hash": ..., "secondary_hash": ...,
              "size": ..., "ranks_count": ..., "missing_ranks_count": ...,
              "ranks": [
                {"rank": 0, "host": ..., "pid": ..., "cuda_dev": ...,
                 "status": {...}, "collective_counts": {...}}, ...
              ],
              "missing_ranks": [
                {"rank": 3, "host": ..., "pid": ...,
                 "status": {"unresponsive": true, "considered_dead": false}}, ...
              ]
            }, ...
          ]
        }

    Args:
        ras_json: The raw ``ncclras -f json`` output.

    Returns:
        The parsed :class:`RASReport`, or ``None`` if the parsing failed.
    """
    try:
        data = json.loads(ras_json)
    except (json.JSONDecodeError, TypeError):
        try:
            # NCCL 2.28.9 JSON ``missing_ranks[]`` has a missing comma, try repair with regex.
            repaired = _MISSING_COMMA_RE.sub(r"\1,\2\3", ras_json)
            data = json.loads(repaired)
        except (json.JSONDecodeError, TypeError) as e:
            logger.info("Parsing NCCL RAS failed with %s", e)
            return None

    try:
        comm_collective_counts: Dict[CommKey, Dict[int, Dict[str, int]]] = {}
        comm_rank_status: Dict[CommKey, Dict[int, str]] = {}
        comm_rank_info: Dict[CommKey, Dict[int, Dict[str, Any]]] = {}
        comm_missing_ranks: Dict[CommKey, Dict[int, Dict[str, Any]]] = {}

        for comm in data["communicators"]:
            key: CommKey = (comm["hash"], comm.get("secondary_hash", ""))
            comm_collective_counts[key] = {
                rank["rank"]: {
                    name: int(count)
                    for name, count in rank["collective_counts"].items()
                }
                for rank in comm["ranks"]
            }
            comm_rank_status[key] = {
                rank["rank"]: RASReport.rank_status(rank["status"])
                for rank in comm["ranks"]
            }
            comm_rank_info[key] = {
                rank["rank"]: {
                    "host": rank.get("host"),
                    "pid": rank.get("pid"),
                    "cuda_dev": rank.get("cuda_dev"),
                }
                for rank in comm["ranks"]
            }

            missing = {}
            for rank in comm.get("missing_ranks", []):
                status = rank.get("status", {})
                missing[rank["rank"]] = {
                    "unresponsive": bool(status.get("unresponsive", False)),
                    "considered_dead": bool(status.get("considered_dead", False)),
                    "host": rank.get("host"),
                    "pid": rank.get("pid"),
                }
            if missing:
                comm_missing_ranks[key] = missing

        return RASReport(
            data["timestamp"],
            comm_collective_counts,
            comm_rank_status,
            comm_rank_info,
            comm_missing_ranks,
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.info(
            "NCCL RAS JSON did not match the expected schema: %s",
            e,
        )
        return None


def capture_command_output(cmd: List[str], timeout_s: float) -> Tuple[bool, str]:
    """Run a diagnostic command on the current process's node, fail-soft.

    Shared subprocess boilerplate for the worker-side diagnostic dumps
    (``py-spy``, ``nvidia-smi``). :func:`run_ncclras` deliberately does not use
    this: its failures are machine-consumed reason codes (``binary_not_found``,
    ``exit_N``, ...) that drive the detector's degradation policy, not
    human-readable text.

    Args:
        cmd: The command line to run; ``cmd[0]`` names the binary in reasons.
        timeout_s: Timeout for the subprocess.

    Returns:
        ``(True, stdout)`` on a zero exit with output, else ``(False, reason)``
        where ``reason`` is a short human-readable explanation.
    """
    binary = cmd[0]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except FileNotFoundError:
        return False, f"{binary} not installed"
    except subprocess.TimeoutExpired:
        return False, f"{binary} timed out"
    except Exception as e:  # noqa: BLE001
        return False, f"{binary} error: {e}"

    if proc.returncode == 0 and proc.stdout.strip():
        return True, proc.stdout
    return False, (proc.stderr or "").strip() or f"{binary} exited {proc.returncode}"


def dump_stack_trace(pyspy_timeout_s: float) -> str:
    """Dump native + Python stacks of the current (worker) process.

    Args:
        pyspy_timeout_s: Timeout for the ``py-spy dump`` subprocess.

    Returns:
        The captured stack trace, or a Python-only traceback (prefixed with the
        reason py-spy was skipped) when py-spy is unavailable.
    """
    ok, output = capture_command_output(
        ["py-spy", "dump", "--pid", str(os.getpid()), "--native"], pyspy_timeout_s
    )
    if ok:
        return output

    # Python-only fallback: dump every thread's stack (cannot show C/C++ trace).
    import sys
    import traceback

    lines = [f"[py-spy unavailable: {output}; Python-only traceback follows]"]
    for thread_id, frame in sys._current_frames().items():
        lines.append(f"\n# Thread {thread_id}")
        lines.append("".join(traceback.format_stack(frame)))
    return "\n".join(lines)


def dump_flight_recorder() -> Tuple[bool, str]:
    """Dump the PyTorch NCCL flight recorder ring buffer on the current worker.

    The flight recorder is a torch-internal ring buffer of the most recent NCCL
    collectives (armed via ``TORCH_NCCL_TRACE_BUFFER_SIZE`` before the process
    group is created). Only the JSON entry points are used: the pickle-based
    ``_dump_nccl_trace`` payload is an unstable torch-internal dict that users
    would have to unpickle to inspect, while the JSON dump is safe to load
    anywhere. On torch >= 2.11 (Ray Train's floor) both JSON symbols exist:
    ``_dump_nccl_trace_json`` (torch >= 2.5) in NCCL-enabled builds
    (``USE_C10D_NCCL``) and the backend-agnostic ``_dump_fr_trace_json``
    (torch >= 2.8) in every build.

    Returns:
        ``(True, json_payload)`` on success, or ``(False, reason)`` if the
        flight recorder could not be dumped.
    """
    try:
        import torch  # noqa: F401
        import torch._C._distributed_c10d as c10d
        import torch.distributed.distributed_c10d as dist_c10d
    except Exception as e:  # noqa: BLE001
        return False, f"torch unavailable: {e}"

    # depending on torch version, the symbol may live on either the public-ish
    # distributed_c10d module or the C extension
    failures: List[str] = []
    for name in ("_dump_nccl_trace_json", "_dump_fr_trace_json"):
        for module in (dist_c10d, c10d):
            fn = getattr(module, name, None)
            if fn is None:
                continue

            try:
                payload = fn()
                if isinstance(payload, bytes):
                    payload = payload.decode()
                return True, payload
            except Exception as e:  # noqa: BLE001
                failures.append(f"{module.__name__}.{name} failed: {e}")

    if failures:
        return False, "; ".join(failures)
    return False, "no flight-recorder JSON dump symbol found in this torch build"


def dump_nvidia_smi() -> str:
    """Capture ``nvidia-smi -q`` output on the current worker's node.

    Returns:
        The full ``nvidia-smi -q`` report, or a short reason string prefixed
        with ``[nvidia-smi unavailable: ...]`` when it could not be run.
    """
    ok, output = capture_command_output(
        ["nvidia-smi", "-q"], _DIAGNOSTIC_DUMP_TIMEOUT_S
    )
    if ok:
        return output
    return f"[nvidia-smi unavailable: {output}]"


class NvHangDetectorCallback(WorkerGroupCallback, ControllerCallback):
    """Detects NCCL hangs via the RAS subsystem (see module docstring for the
    topology and the known blind spot).

    Opt-in: ``DataParallelTrainer`` registers this callback only when
    ``RAY_TRAIN_ENABLE_NV_HANG_DETECTOR=1`` is set. RAS exists only for NCCL,
    so enable it only for NCCL (GPU) jobs -- on any other backend every poll
    fails until the detector disables itself.

    To confirm that a NCCL anomaly isn't a single-snapshot blip, a communicator
    must stay *frozen* -- no rank advancing any collective -- for at least
    ``RAY_TRAIN_NCCL_RAS_CONFIRM_WINDOW_S`` seconds. Each communicator is
    tracked and confirmed on its own wall-clock timer
    (:attr:`comm_frozen_since`), so a query that fails or is skipped cannot
    silently shorten or stretch the window. A healthy poll clears all timers.

    Configuration note: the ``RAY_TRAIN_NCCL_RAS_*`` knobs and
    ``TORCH_NCCL_TRACE_BUFFER_SIZE`` are read once here, on the *driver*, at
    construction and pickled to the controller. ``NCCL_RAS_ADDR`` is instead
    read on the *workers* at query time (inside :func:`run_ncclras`), so it must
    be present in the worker environment, not just the driver's.
    """

    def __init__(self):
        self._action = os.environ.get(NCCL_RAS_ACTION_ENV_VAR, DEFAULT_NCCL_RAS_ACTION)
        if self._action not in (NCCL_RAS_ACTION_FAIL, NCCL_RAS_ACTION_OBSERVE):
            raise ValueError(
                f"{NCCL_RAS_ACTION_ENV_VAR} must be one of "
                f"{NCCL_RAS_ACTION_FAIL!r} or {NCCL_RAS_ACTION_OBSERVE!r}, "
                f"got {self._action!r}."
            )

        self._poll_interval_s = env_float(
            NCCL_RAS_POLL_INTERVAL_S_ENV_VAR, DEFAULT_NCCL_RAS_POLL_INTERVAL_S
        )
        self._confirm_window_s = env_float(
            NCCL_RAS_CONFIRM_WINDOW_S_ENV_VAR, DEFAULT_NCCL_RAS_CONFIRM_WINDOW_S
        )
        if self._confirm_window_s <= 0:
            raise ValueError(
                f"{NCCL_RAS_CONFIRM_WINDOW_S_ENV_VAR} must be a positive "
                f"number of seconds, got {self._confirm_window_s}."
            )

        self._nccl_ras_binary_path = os.environ.get(
            NCCLRAS_BINARY_PATH_ENV_VAR, DEFAULT_NCCLRAS_BINARY_PATH
        )
        self._flight_recorder_enabled = (
            env_integer(TORCH_NCCL_TRACE_BUFFER_SIZE_ENV_VAR, 0) > 0
        )

        # Monotonic clock (indirected so tests can inject a deterministic clock).
        self._clock = time.monotonic

        # The train worker group to query for RAS.
        self._worker_group: Optional[WorkerGroup] = None
        # Background executor for RAS queries so they never block the controller loop.
        self._executor: Optional[ThreadPoolExecutor] = None
        # In-flight background JSON RAS query (see ``poll_ras_report``), if any.
        self._ras_query_future: Optional[Future] = None
        # Force a query on the next poll after (re)start.
        self._last_query_time = float("-inf")

        # One-time FATAL degradation (e.g. missing/outdated binary): latched for the whole run
        self._is_ras_degraded: bool = False

        self.reset_detection_state()

    def reset_detection_state(self):
        """Full worker-group lifecycle reset (on (re)start / shutdown).

        Clears the frozen-comm timers and the *transient* degradation state so a
        one-off detector error or a temporary RAS outage does not permanently
        disable detection across an elastic recovery. Fatal degradation
        (``_is_ras_degraded``) is intentionally left latched.
        """
        self._ras_query_future = None
        self._last_query_time = float("-inf")
        self.prev_report: Optional[RASReport] = None

        # Transient degradation, reset on restart (detector bug / RAS unreachable).
        self._ras_transient_disabled: bool = False
        self._consecutive_ras_query_failures: int = 0

        # Rolling window of the last N raw JSON reports for postmortem timeline
        self._ras_report_history: Deque[str] = deque(maxlen=_RAW_REPORT_HISTORY_SIZE)

        self.reset_hang_counters()

    def reset_hang_counters(self):
        """Clear per-communicator hang state after a healthy poll or restart.

        ``prev_report`` is intentionally left in place by the healthy-poll caller
        so the next poll still has a baseline to diff against.
        """
        # Monotonic time each still-frozen communicator was first seen frozen.
        self.comm_frozen_since: Dict[CommKey, float] = {}
        # Communicators we've already emitted the first-suspicion warning for.
        self._suspicion_announced: Set[CommKey] = set()
        # Communicators already confirmed as hung (latched so observe mode logs once).
        self._confirmed_comms: Set[CommKey] = set()
        # Last time the periodic "still suspected" reminder was emitted.
        self._last_periodic_warn_s = float("-inf")

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._worker_group = worker_group
        self.reset_detection_state()

    def before_worker_group_shutdown(self, worker_group):
        self._worker_group = None
        # Abandon any in-flight query.
        if self._ras_query_future is not None:
            self._ras_query_future.cancel()
            self._ras_query_future = None

    async def before_controller_shutdown(self):
        # Tear down the background query executor so its thread doesn't leak.
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None

    def after_worker_group_poll_status(self, worker_group_status):
        if (
            self._is_ras_degraded
            or self._ras_transient_disabled
            or self._worker_group is None
        ):
            return

        # This hook runs on the controller's poll loop, so any error here must never crash training.
        try:
            ras_report = self.poll_ras_report()
            if ras_report is None:
                # No fresh query this poll due to throttling or an in-flight query.
                return

            if (
                self.prev_report is not None
                and ras_report.timestamp == self.prev_report.timestamp
            ):
                # Stale snapshot from a wedged RAS agent
                return

            if not ras_report.healthy:
                self.evaluate_comm_mismatch(ras_report)
            else:  # Healthy: no mismatches and no unresponsive ranks.
                self.log_resumed_comms({}, self._clock())
                self.reset_hang_counters()

            self.prev_report = ras_report
        except NCCLHangError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception(
                "NCCL RAS hang detection hit an unexpected error, therefore, "
                "disabling it for the rest of this training run."
            )
            self._ras_transient_disabled = True

    def evaluate_comm_mismatch(self, report: RASReport):
        """Track frozen communicators and escalate user-facing hang messaging.

        A communicator is *frozen* only when no rank advanced any collective
        since the last poll: a real hang blocks every rank, so every launch
        count freezes. A merely skewed-but-advancing communicator is alive and
        is not treated as a hang.
        """
        if self.prev_report is None:
            return

        now = self._clock()
        collective_diff = compute_report_collective_diff(self.prev_report, report)

        # Consider both skew-mismatched comms and comms with an unresponsive rank.
        candidates = report.mismatched_comms | report.unresponsive_comms

        still_frozen: Dict[CommKey, float] = {}
        for comm in candidates:
            comm_diff = collective_diff.get(comm)
            if not comm_diff:
                # No shared/baseline ranks to diff (e.g. comm just appeared, or
                # only non-overlapping ranks): can't call it frozen this poll.
                continue

            frozen = all(
                delta == 0
                for rank_deltas in comm_diff.values()
                for delta in rank_deltas.values()
            )
            if not frozen:
                continue

            # Preserve the timestamp from when this comm first became frozen.
            still_frozen[comm] = self.comm_frozen_since.get(comm, now)

        # Drop resumed comms (and announce recovery); keep the frozen ones.
        self.log_resumed_comms(still_frozen, now)

        # Confirm any communicator frozen for at least the confirm window that we
        # have not already confirmed. Latch confirmations so observe mode fires
        # its diagnostics exactly once.
        confirmed = [
            comm
            for comm, since in self.comm_frozen_since.items()
            if (now - since) >= self._confirm_window_s
            and comm not in self._confirmed_comms
        ]
        if confirmed:
            self._confirmed_comms.update(confirmed)
            self.on_confirmed_hang(report, confirmed, now)

        # Escalation ladder for comms that are frozen but not yet confirmed.
        self.emit_escalation_warnings(report, now)

    def emit_escalation_warnings(self, report: RASReport, now: float):
        """First-suspicion and periodic "still suspected" warnings (time-based).

        Communicators already confirmed as hung are excluded: when the confirm
        window is shorter than the first-suspicion threshold, confirmation fires
        first and a trailing "possible hang" warning for the same comm would
        only be confusing.
        """
        if not self.comm_frozen_since:
            return

        total_comms = len(report.comm_collective_counts)

        # First-suspicion announcement: each comm announced once when it crosses
        # the first-suspicion window.
        newly_suspected = [
            comm
            for comm, since in self.comm_frozen_since.items()
            if (now - since) >= _FIRST_SUSPICION_S
            and comm not in self._suspicion_announced
            and comm not in self._confirmed_comms
        ]
        if newly_suspected:
            # TODO: emit a run annotation here (suspicion) once the Train
            # annotation API lands.
            self._suspicion_announced.update(newly_suspected)
            escalation = (
                f"A NCCLHangError will be raised after {self._confirm_window_s:.0f} "
                "seconds of no progress if this persists."
                if self._action == NCCL_RAS_ACTION_FAIL
                else ""
            )
            logger.warning(
                "Possible NCCL hang detected! %d of %d communicators (%s) have "
                "made no progress for %.0f seconds. Continuing to monitor, this "
                "might be a transient stall. %s",
                len(newly_suspected),
                total_comms,
                ", ".join(format_comm(comm) for comm in newly_suspected),
                _FIRST_SUSPICION_S,
                escalation,
            )

        # Periodic reminder for every still-suspected comm, rate-limited to one
        # message per ``_PERIODIC_WARN_EVERY_S`` and fetching the RAS report once.
        suspected = {
            comm: since
            for comm, since in self.comm_frozen_since.items()
            if (now - since) >= _FIRST_SUSPICION_S and comm not in self._confirmed_comms
        }
        if suspected and (now - self._last_periodic_warn_s) >= _PERIODIC_WARN_EVERY_S:
            self._last_periodic_warn_s = now
            stalled = ", ".join(
                f"{format_comm(comm)} for {now - since:.0f}s"
                for comm, since in suspected.items()
            )
            periodic_escalation = ""
            if self._action == NCCL_RAS_ACTION_FAIL:
                longest_frozen_s = max(now - since for since in suspected.values())
                remaining_s = max(0.0, self._confirm_window_s - longest_frozen_s)
                periodic_escalation = (
                    f"A NCCLHangError will be raised in {remaining_s:.0f} seconds "
                    "if this persists."
                )
            logger.warning(
                "NCCL hang still suspected! %d of %d communicators (%s) have made "
                "no progress. %s",
                len(suspected),
                total_comms,
                stalled,
                periodic_escalation,
            )
            ras_human_output = self.fetch_ras_text_report()
            if ras_human_output:
                logger.info("%s", ras_human_output)

    def on_confirmed_hang(
        self, report: RASReport, confirmed_comms: List[CommKey], now: float
    ):
        """Handle a confirmed hang for both actions.

        Emits one loud, latched error naming the culprit ranks, captures the
        human-readable RAS report and per-rank diagnostics (stack traces plus, if
        enabled, the flight recorder and ``nvidia-smi``), then -- for the ``fail``
        action -- raises a terminal :class:`NCCLHangError`. In ``observe`` mode
        the run continues. Every action runs the same diagnostics so a dashboard
        or postmortem always has something to key on.
        """
        total_comms = len(report.comm_collective_counts)
        longest_frozen_s = max(
            now - self.comm_frozen_since[comm] for comm in confirmed_comms
        )
        comm_desc = ", ".join(format_comm(comm) for comm in confirmed_comms)

        # Attribute the hang to specific ranks so the error is actionable.
        worker_failures, culprit_messages = self.identify_culprit_ranks(
            report, confirmed_comms
        )
        culprit_desc = (
            "\n".join(f"  - {msg}" for msg in culprit_messages)
            if culprit_messages
            else "  - (RAS could not attribute the hang to specific ranks)"
        )

        # TODO: emit a run annotation here (confirmation) once the Train
        # annotation API lands.
        logger.error(
            "NCCL hang confirmed: %d of %d communicators (%s) made no progress "
            "for %.0f seconds. Suspected culprit rank(s):\n%s",
            len(confirmed_comms),
            total_comms,
            comm_desc,
            longest_frozen_s,
            culprit_desc,
        )

        ras_human_output = self.fetch_ras_text_report()
        if ras_human_output:
            logger.error("%s", ras_human_output)

        # Persist the postmortem diagnostics -- per-rank stack traces, the RAS
        # report timeline, and any flight-recorder / nvidia-smi dumps -- in a
        # single artifact folder. Fail-soft so it can never mask the hang or
        # crash observe mode.
        try:
            dump_dir = self.save_diagnostic_artifacts(ras_human_output)
        except Exception:  # noqa: BLE001
            logger.exception("Trying to persist NCCL hang diagnostics failed.")
            dump_dir = None

        if self._action != NCCL_RAS_ACTION_FAIL:
            # observe mode: diagnostics captured above; let the run continue.
            return

        debug_lines = [
            "  - NCCL report in the logs (identifies the stalled ranks/communicators).",
        ]
        if dump_dir is not None:
            debug_lines.append(
                f"  - Folder with the per-rank stack traces and RAS artifacts: {dump_dir}"
            )

        raise NCCLHangError(
            f"NCCL hang detector: {len(confirmed_comms)} of {total_comms} communicators made no progress for {longest_frozen_s:.0f} seconds. "
            "This usually means the ranks disagreed on a collective, e.g. a rank hit a divergent code path or exited early, a hardware failure between workers or nodes, or a collective was launched with a mismatched shape, dtype, or call order.\n"
            f"Suspected culprit rank(s):\n{culprit_desc}\n"
            f"To debug:\n" + "\n".join(debug_lines) + "\n",
            worker_failures=worker_failures,
        )

    def log_resumed_comms(self, still_frozen: Dict[CommKey, float], now: float):
        """Announce communicators that were suspected but have resumed progress.

        Drops any communicator no longer frozen from the streak timers and, for
        those that had crossed the first-suspicion threshold, logs that they've
        recovered so a watching user isn't left wondering.
        """
        for comm, since in self.comm_frozen_since.items():
            if comm not in still_frozen and (now - since) >= _FIRST_SUSPICION_S:
                logger.info(
                    "NCCL communicator %s resumed making progress after being "
                    "frozen for %.0f seconds. It is no longer suspected of hanging.",
                    format_comm(comm),
                    now - since,
                )
        self._suspicion_announced &= set(still_frozen)
        self.comm_frozen_since = still_frozen

    def identify_culprit_ranks(
        self, report: RASReport, confirmed_comms: List[CommKey]
    ) -> Tuple[Dict[int, Exception], List[str]]:
        """Attribute a confirmed hang to the laggard / unresponsive ranks.

        For each confirmed communicator, and for every collective whose counts
        disagree, the ranks that launched the fewest of it (the stragglers every
        other rank is blocked waiting on) and any ranks RAS marked unresponsive
        are named, with their host / pid / cuda device so an operator can jump
        straight to the offending process.

        RAS reports ranks by their *communicator-local* rank, which for a
        sub-communicator does not match the training world rank. Each culprit's
        RAS ``(host, pid)`` is therefore joined against the worker group's actor
        metadata to recover its world rank. Culprits that match no worker (e.g.
        a NCCL process outside this worker group) still appear in the returned
        messages but are left out of the world-rank keyed dict.

        Args:
            report: The RAS report at confirmation time.
            confirmed_comms: The communicators confirmed as hung.

        Returns:
            Tuple of:
            - ``{world_rank: NCCLRankHang(...)}`` for the culprits matched to a
              worker, suitable for :attr:`NCCLHangError.worker_failures`.
            - Every human-readable culprit message, matched to a worker or not.
        """
        # (host, pid) -> world rank; RAS may report the node IP or the hostname.
        world_rank_by_host_pid: Dict[Tuple[Any, Any], int] = {}
        for world_rank, worker in enumerate(self.current_workers()):
            metadata = worker.metadata
            world_rank_by_host_pid[(metadata.node_ip, metadata.pid)] = world_rank
            world_rank_by_host_pid[(metadata.hostname, metadata.pid)] = world_rank

        messages: List[str] = []
        # A world rank can be a culprit on several confirmed communicators.
        world_rank_messages: Dict[int, List[str]] = defaultdict(list)

        def format_rank_info(info: Optional[Dict[str, Any]]) -> str:
            """Render a rank's ``{host, pid, cuda_dev}`` for a human-readable message."""
            if not info:
                return "(location unknown)"
            parts = []
            if info.get("host") is not None:
                parts.append(f"host {info['host']}")
            if info.get("pid") is not None:
                parts.append(f"pid {info['pid']}")
            if info.get("cuda_dev") is not None:
                parts.append(f"cuda:{info['cuda_dev']}")
            return f"({', '.join(parts)})" if parts else "(location unknown)"

        def add_culprit(
            comm: CommKey,
            local_rank: int,
            rank_info: Optional[Dict[str, Any]],
            reason: str,
        ):
            world_rank = None
            if rank_info:
                world_rank = world_rank_by_host_pid.get(
                    (rank_info.get("host"), rank_info.get("pid"))
                )
            who = (
                f"World rank {world_rank}"
                if world_rank is not None
                else "An unidentified worker"
            )
            msg = (
                f"{who} {format_rank_info(rank_info)}, rank {local_rank} of "
                f"communicator {format_comm(comm)}, {reason}."
            )
            messages.append(msg)
            if world_rank is not None:
                world_rank_messages[world_rank].append(msg)

        for comm in confirmed_comms:
            counts = report.comm_collective_counts.get(comm, {})
            info = report.comm_rank_info.get(comm, {})
            skews = report.comm_collective_skews.get(comm, {})

            # Report every skewed collective points at stragglers (widest first)
            for collective, spread in sorted(
                skews.items(), key=lambda kv: (-kv[1], kv[0])
            ):
                if spread <= 0:
                    break  # no more collectives should be skewed
                per_rank = {
                    rank: rank_counts.get(collective, 0)
                    for rank, rank_counts in counts.items()
                }
                leader = max(per_rank.values())
                for rank, count in per_rank.items():
                    if count < leader:
                        add_culprit(
                            comm,
                            rank,
                            info.get(rank),
                            f"is behind by {leader - count} {collective} launch(es)",
                        )

            # Unresponsive ranks are culprits even at zero skew.
            for rank, missing in report.comm_missing_ranks.get(comm, {}).items():
                if missing.get("unresponsive"):
                    dead = (
                        " (considered dead)" if missing.get("considered_dead") else ""
                    )
                    add_culprit(comm, rank, missing, f"is unresponsive{dead}")

        worker_failures = {
            world_rank: NCCLRankHang("\n".join(msgs))
            for world_rank, msgs in world_rank_messages.items()
        }
        return worker_failures, messages

    def save_diagnostic_artifacts(self, ras_text: Optional[str]) -> Optional[str]:
        """Collect and persist every hang diagnostic in one artifact folder.

        Produces the layout documented in the module docstring under
        :data:`_HANG_ARTIFACT_DIR`: one py-spy dump per responding worker under
        ``stack_traces/``, the human-readable ``-f text`` report plus each
        retained raw JSON report (named by its RAS timestamp, forming a
        postmortem timeline) under ``nccl_ras/``, one ``nvidia-smi -q`` dump per
        *node* under ``nvidia_smi/``, and -- when the flight recorder is armed
        -- a per-rank JSON trace under ``flight_recorder/``. Everything is
        staged in one temporary directory and uploaded to storage once.

        Args:
            ras_text: The human-readable ``-f text`` report, if it was fetched.

        Returns:
            The artifact folder path, or ``None`` if nothing could be written.
        """
        if self._worker_group is None:
            return None

        with tempfile.TemporaryDirectory() as temp_dir:
            tmp = Path(temp_dir)

            def write_artifact(subdir: str, filename: str, content: str):
                path = tmp / subdir / filename
                path.parent.mkdir(exist_ok=True)
                path.write_text(content)

            # Per-rank native + Python stack dumps (what is every rank stuck on?).
            stacks = self.collect_from_workers(
                dump_stack_trace, _DIAGNOSTIC_DUMP_TIMEOUT_S - 5
            )
            if not stacks:
                logger.info("Could not collect a stack dump from any worker.")
            for rank, stack_trace in stacks.items():
                write_artifact("stack_traces", f"{rank}.log", stack_trace)

            if ras_text:
                write_artifact("nccl_ras", "report.txt", ras_text)

            # One raw JSON report per retained poll, named by the report's own
            # RAS timestamp so the folder reads as a timeline.
            used_names: Set[str] = set()
            for index, raw in enumerate(self._ras_report_history):
                try:
                    timestamp = json.loads(raw).get("timestamp")
                except (json.JSONDecodeError, TypeError):
                    timestamp = None
                name = (
                    re.sub(r"[^A-Za-z0-9._-]+", "_", str(timestamp))
                    if timestamp
                    else f"poll_{index}"
                )
                if name in used_names:
                    name = f"{name}_{index}"
                used_names.add(name)
                write_artifact("nccl_ras", f"{name}.log", raw)

            # Node GPU health: nvidia-smi describes every GPU on the node, so
            # one dump per node (not per worker).
            for node_ip, output in self.collect_nvidia_smi_per_node().items():
                write_artifact("nvidia_smi", f"{node_ip}.log", output)

            if self._flight_recorder_enabled:
                for rank, (ok, payload) in self.collect_from_workers(
                    dump_flight_recorder
                ).items():
                    if not ok:
                        payload = f"[flight recorder unavailable: {payload}]"
                    write_artifact("flight_recorder", f"{rank}.log", payload)

            if not any(tmp.iterdir()):
                return None

            storage = self._worker_group._storage_context
            folder = os.path.join(storage.experiment_fs_path, _HANG_ARTIFACT_DIR)
            _upload_to_fs_path(temp_dir, storage.storage_filesystem, folder)
            return folder

    def collect_nvidia_smi_per_node(self) -> Dict[str, str]:
        """Run ``nvidia-smi -q`` once per *node*, via one probe worker per node.

        Returns:
            ``{node_ip: nvidia-smi output}`` for the nodes that responded.
        """
        probes = []
        seen_nodes: Set[str] = set()
        for worker in self.current_workers():
            if worker.metadata.node_id not in seen_nodes:
                seen_nodes.add(worker.metadata.node_id)
                probes.append(worker)

        results = self.collect_from_workers(dump_nvidia_smi, workers=probes)
        return {probes[i].metadata.node_ip: output for i, output in results.items()}

    def poll_ras_report(self) -> Optional[RASReport]:
        """Drive the throttled JSON RAS poll without blocking the event loop.

        Only the periodic JSON poll goes through here. The one-off human-readable
        ``-f text`` report is fetched via :meth:`fetch_ras_text_report` so it doesn't
        share this method's single-in-flight future or poll-interval throttle.

        Returns:
            A report on the poll where a query becomes ready, else ``None``.
        """
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="nccl-ras-query"
            )

        now = self._clock()
        if self._ras_query_future is None and (
            now - self._last_query_time >= self._poll_interval_s
        ):
            self._last_query_time = now
            self._ras_query_future = self._executor.submit(
                self.query_ras_on_workers, "json"
            )

        future = self._ras_query_future
        if future is not None and future.done():
            self._ras_query_future = None
            try:
                return future.result()
            except Exception as e:
                logger.info("ncclras query failed: %s", e)
        return None

    def fetch_ras_text_report(self) -> Optional[str]:
        """Fetch the human-readable ``-f text`` RAS report off the controller loop.

        Runs on the background executor over a bounded set of workers with a
        bounded total wait, so a wedged RAS listener can't stall the controller.

        Returns:
            The report text, or ``None`` if it couldn't be fetched in time.
        """
        if self._executor is None:
            return None
        try:
            future = self._executor.submit(
                self.query_ras_on_workers, "text", _TEXT_QUERY_MAX_WORKERS
            )
            return future.result(timeout=_NCCL_RAS_QUERY_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001
            logger.debug("NCCL RAS text report fetch failed: %s", e)
            return None

    def query_ras_on_workers(
        self,
        ras_format: Literal["json", "text"],
        max_workers: Optional[int] = None,
    ) -> Optional[Union[RASReport, str]]:
        """Run a RAS query across candidate workers (runs on the background thread).

        Tries each worker in turn and returns the first usable report. If every
        worker fails, logs the reason: fatal misconfigurations (missing/outdated
        ``ncclras`` binary) disable detection for the rest of the run, while
        transient failures (timeouts, exit codes) are retried on the next poll --
        unless they persist for ``_MAX_CONSECUTIVE_QUERY_FAILURES`` polls, at
        which point RAS is treated as unreachable.

        Args:
            ras_format: What format to use with ``ncclras``.
            max_workers: If set, only try the first ``max_workers`` workers (used
                to bound the human-readable fetch).

        Returns:
            The parsed report (``json``) / raw text (``text``), or ``None`` if no
            worker produced a usable result this poll.
        """
        workers = self.current_workers()
        if max_workers is not None:
            workers = workers[:max_workers]
        if not workers:
            logger.warning(
                "NCCL RAS: no workers available to query `ncclras`. "
                "Skipping this poll."
            )
            return None

        last_failure_reason: Optional[str] = None
        for worker in workers:
            ref = None
            try:
                ref = worker.execute_async(
                    run_ncclras,
                    self._nccl_ras_binary_path,
                    _NCCL_RAS_QUERY_TIMEOUT_S,
                    ras_format,
                )
                result = ray.get(ref, timeout=_NCCL_RAS_QUERY_TIMEOUT_S)
            except GetTimeoutError:
                last_failure_reason = "query_timeout"
                logger.debug(
                    "NCCL RAS: `ncclras` query timed out on worker %s. Cancelling and trying the next worker.",
                    worker,
                )
                if ref is not None:
                    ray.cancel(ref)
                continue
            except Exception as e:  # noqa: BLE001
                last_failure_reason = f"query_error: {e}"
                logger.debug(
                    "NCCL RAS: `ncclras` query failed on worker %s: %s. Trying the next worker.",
                    worker,
                    e,
                )
                if ref is not None:
                    ray.cancel(ref)
                continue

            if not result.get("ok"):
                last_failure_reason = result.get("reason")
                logger.debug(
                    "NCCL RAS: `ncclras` on worker %s returned no data (%s). Trying the next worker.",
                    worker,
                    result.get("reason"),
                )
                continue

            if ras_format == "json":
                # Stash the raw JSON for the pre-fail snapshot / soft-hang logs.
                logger.debug("NCCL RAS: `ncclras` json output: %s", result["stdout"])
                report = parse_ras_schema(result["stdout"])
                if report is None:
                    last_failure_reason = "unparseable_json"
                    continue
                # Keep the raw JSON for the confirmed-hang postmortem timeline.
                self._ras_report_history.append(result["stdout"])
                self._consecutive_ras_query_failures = 0
                return report
            else:  # ras_format == 'text'
                if result["stdout"]:
                    return result["stdout"]
                last_failure_reason = "empty_text_output"
                continue

        # Every worker failed. Fatal, run-wide misconfigurations disable the
        # detector permanently; a persistent connection failure disables it until
        # restart; anything else is transient and retried next poll.
        if last_failure_reason == "binary_not_found":
            logger.warning(
                "NCCL RAS: `ncclras` binary %r not found on any worker. "
                "Disabling NCCL RAS hang detection for the rest of this run. "
                "Set %s to a valid path.",
                self._nccl_ras_binary_path,
                NCCLRAS_BINARY_PATH_ENV_VAR,
            )
            self._is_ras_degraded = True
        elif last_failure_reason == "unsupported_f_option":
            logger.warning(
                "NCCL RAS: `ncclras` binary %r rejected the `-f` format flag, "
                "which requires NCCL 2.28+. Disabling NCCL RAS hang detection "
                "for the rest of this run. Please update ncclras to 2.28+.",
                self._nccl_ras_binary_path,
            )
            self._is_ras_degraded = True
        elif ras_format == "json":
            self._consecutive_ras_query_failures += 1
            if self._consecutive_ras_query_failures >= _MAX_CONSECUTIVE_QUERY_FAILURES:
                logger.warning(
                    "NCCL RAS: `ncclras` produced no usable data on any of %d "
                    "worker(s) for %d consecutive polls (last reason: %s). "
                    "Treating RAS as unreachable and disabling hang detection "
                    "until the next worker-group restart.",
                    len(workers),
                    self._consecutive_ras_query_failures,
                    last_failure_reason,
                )
                self._ras_transient_disabled = True
            else:
                logger.info(
                    "NCCL RAS: `ncclras` (%s) returned no usable data from any of "
                    "%d worker(s) this poll (last reason: %s). Will retry next poll.",
                    ras_format,
                    len(workers),
                    last_failure_reason,
                )
        else:
            logger.info(
                "NCCL RAS: `ncclras` (%s) returned no usable data from any of "
                "%d worker(s) this poll (last reason: %s).",
                ras_format,
                len(workers),
                last_failure_reason,
            )
        return None

    def current_workers(self) -> List[Worker]:
        """Snapshot the worker group's workers, tolerating a concurrent shutdown.

        :meth:`before_worker_group_shutdown` clears ``_worker_group`` on the
        controller thread while a RAS query or diagnostic dump may still be in
        flight on the background executor (``Future.cancel`` is a no-op once the
        task is running). Reading the reference once and degrading to an empty
        list keeps those in-flight calls fail-soft instead of raising
        ``AttributeError`` on a ``None`` group.

        Returns:
            The workers in the group, in global rank order, or ``[]`` if the
            group has already been torn down.
        """
        worker_group = self._worker_group
        if worker_group is None:
            return []
        return list(worker_group.get_workers())

    def collect_from_workers(
        self,
        worker_fn: Callable,
        *args: Any,
        timeout: float = _DIAGNOSTIC_DUMP_TIMEOUT_S,
        workers: Optional[List[Worker]] = None,
    ) -> Dict[int, Any]:
        """Run ``worker_fn`` on workers and collect results keyed by worker index.

        Fail-soft: workers that can't be launched, time out, or raise are simply
        omitted from the result, so one dead rank can't stop the others' dumps.

        Args:
            worker_fn: The function to run on each worker.
            *args: Positional args forwarded to ``worker_fn``.
            timeout: Total wall-clock budget to wait for all workers.
            workers: The workers to run on. Defaults to every worker in the
                group, in which case the result keys are the global ranks.

        Returns:
            ``{worker_index: worker_fn(...)}`` for the workers that responded.
        """
        if workers is None:
            workers = self.current_workers()
        if not workers:
            return {}

        refs = {}
        for rank, worker in enumerate(workers):
            try:
                refs[worker.execute_async(worker_fn, *args)] = rank
            except Exception as e:  # noqa: BLE001
                logger.info(
                    "Failed to launch %s on worker %s: %s",
                    getattr(worker_fn, "__name__", worker_fn),
                    worker,
                    e,
                )
        if not refs:
            return {}

        ready, not_ready = ray.wait(list(refs), num_returns=len(refs), timeout=timeout)
        for ref in not_ready:
            ray.cancel(ref)

        results: Dict[int, Any] = {}
        for ref in ready:
            try:
                results[refs[ref]] = ray.get(ref)
            except Exception as e:  # noqa: BLE001
                logger.info("Failed to collect from rank %s: %s", refs[ref], e)
        return results
