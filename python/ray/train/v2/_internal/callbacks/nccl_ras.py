"""NCCL RAS-based hang detector callback.

NCCL ships a Reliability/Availability/Serviceability (RAS) subsystem (NCCL
>= 2.24) that runs a monitoring thread inside *every* NCCL process (one per
GPU/rank). Those threads form a peer mesh tracking job health, detecting dead/
unresponsive ranks and per-rank collective op-counts. By polling ``ncclras``
we can check if a communicator's op counts between ranks are mismatched,
indicating a hang if the op counts don't increase over sequential polls.

Warning: RAS only knows about the ranks it can reach through its mesh, and the
mesh is built from the communicators each process has created. Querying
``ncclras`` on one rank therefore returns every rank reachable *from that
rank*, which is the whole job only if some communicator spans all ranks (a
"world" communicator, e.g. the default process group ``torch.distributed``
creates). If a job only ever creates communicators over disjoint subsets of
ranks (say ranks 0-1 and ranks 2-3 with no group over 0-3), a query on rank 0
reports only ranks 0-1 and their communicators, and a hang in the other subset
is invisible to this callback. In practice every parallelism strategy creates
a world process group, so real workloads are unlikely to hit this.
"""
import json
import logging
import math
import os
import queue
import re
import subprocess
import tempfile
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import ray
from ray._private.ray_constants import env_float
from ray.exceptions import GetTimeoutError
from ray.train.v2._internal.constants import (
    DEFAULT_NCCL_RAS_ACTION,
    DEFAULT_NCCL_RAS_CONFIRM_DURATION_S,
    DEFAULT_NCCL_RAS_MIN_POLL_INTERVAL_S,
    HANG_DETECTOR_DIRNAME,
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_ADDR_ENV_VAR,
    NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR,
    NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR,
    TORCH_FR_BUFFER_SIZE_ENV_VAR,
)
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.storage import _upload_to_fs_path
from ray.train.v2._internal.execution.worker_group import Worker, WorkerGroup
from ray.train.v2._internal.metrics.base import GaugeMetric
from ray.train.v2._internal.metrics.controller import (
    NcclHangDetectorMetrics,
    NcclHangDetectorState,
)
from ray.train.v2._internal.state.schema import (
    NcclCommunicatorHangState,
    NcclCommunicatorState,
    NcclHangDetectorStatus,
    NcclHangStopReason,
    NcclHangTrigger,
    StalledRanks,
    TrainNcclHangEvent,
)
from ray.train.v2._internal.state.state_manager import NcclHangEventExporter
from ray.train.v2.api.exceptions import NCCLHangError

logger = logging.getLogger(__name__)

# Query timeout lengths
_STACK_DUMP_TIMEOUT_S: float = 30.0
_NVIDIA_SMI_TIMEOUT_S: float = 30.0
_FLIGHT_RECORDER_DUMP_TIMEOUT_S: float = 30.0
_NCCL_RAS_QUERY_TIMEOUT_S: float = 8.0  # the default ncclras -t value is 5

# Every diagnostic is uploaded to
# `<experiment_fs_path>/<HANG_DETECTOR_DIRNAME>/<tool>/`.
_STACK_TRACES_TOOL: str = "stack_traces"
_NCCL_RAS_TOOL: str = "nccl_ras"
_NVIDIA_SMI_TOOL: str = "nvidia_smi"
_FLIGHT_RECORDER_TOOL: str = "flight_recorder"

# Polls of RAS history kept on top of the ones a confirmation consumes, so the
# saved history always starts before the communicator stalled.
_RAS_HISTORY_MARGIN_POLLS: int = 10

# Characters not kept when a RAS timestamp is turned into a filename.
_UNSAFE_FILENAME_CHARS = re.compile(r"[^0-9A-Za-z._-]")

# User-facing escalation milestones
_FIRST_SUSPICION_AFTER_S: float = 60.0
_PERIODIC_WARN_EVERY_S: float = 120.0

# Rank ranges named per list in a log line; the rest are summarised as a
# count so a 1000-rank job doesn't log 1000 ranks per poll.
_MAX_RANKS_LOGGED: int = 8


def parse_ras_addr(addr: str) -> Tuple[str, int]:
    """Parse an ``NCCL_RAS_ADDR`` value (``host:port``) into ``(host, port)``.

    Handles bare hosts (default port), ``host:port``, and bracketed IPv6 such
    as ``[::1]:28028``.

    Args:
        addr: The ``NCCL_RAS_ADDR`` value to parse.

    Returns:
        Tuple of ``(host, port)``
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
    cmd = [binary_path, "-f", fmt, "-t", str(int(timeout_s))]
    # Only override the address when the user set NCCL's variable; otherwise
    # let `ncclras` use its own built-in default.
    ras_addr = os.environ.get(NCCL_RAS_ADDR_ENV_VAR)
    if ras_addr:
        host, port = parse_ras_addr(ras_addr)
        cmd += ["-h", host, "-p", str(port)]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
    except FileNotFoundError:
        return {"ok": False, "reason": "binary_not_found"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "reason": f"timed out ({timeout_s})"}
    except Exception as e:
        return {"ok": False, "reason": f"error: {e}"}

    if proc.returncode != 0:
        stderr = proc.stderr or ""
        if "invalid option -- 'f'" in stderr:
            reason = "unsupported_f_option"
        else:
            reason = f"exit_{proc.returncode}"

        return {
            "ok": False,
            "reason": reason,
            "stderr": stderr[:500],
        }

    return {"ok": True, "stdout": proc.stdout}


@dataclass
class RASReport:
    """Structured summary of an ``ncclras`` JSON report.

    Attributes:
        timestamp: str, when `ncclras` responded.
        comm_op_counts: Maps each communicator id to ``{global_rank: {op_name:
            count}}`` for ALL of that communicator's ranks (no filtering, no
            majority logic).
        comm_rank_status: Maps each communicator and their ranks with their
            status. A hang requires that the rank to be RUNNING.
        comm_rank_processes: Maps each communicator and their ranks to the
            ``(host, pid)`` of the process running that rank, used to find the
            Ray Train worker behind a communicator rank.
        raw_json: The ``ncclras`` output this report was parsed from
    """

    timestamp: str
    comm_op_counts: Dict[str, Dict[int, Dict[str, int]]]
    comm_rank_status: Dict[str, Dict[int, str]]
    comm_rank_processes: Dict[str, Dict[int, Tuple[str, int]]]
    raw_json: str

    def op_skews(self, comm_id: str) -> Dict[str, int]:
        """A communicator's per-op spread (max-min) across its ranks, this poll.

        Args:
            comm_id: A communicator in this report.

        Returns:
            ``{op_name: max_count - min_count for ranks}``.
        """
        rank_op_counts = defaultdict(list)
        for op_counts in self.comm_op_counts[comm_id].values():
            for op, count in op_counts.items():
                rank_op_counts[op].append(count)
        return {op: max(counts) - min(counts) for op, counts in rank_op_counts.items()}

    @cached_property
    def comm_op_skews(self) -> Dict[str, Dict[str, int]]:
        """Per-communicator, per-op spread (max-min) across its ranks, this poll.

        Returns:
            ``{comm_id: {op_name: max_count - min_count for ranks}}``.
        """
        return {comm_id: self.op_skews(comm_id) for comm_id in self.comm_op_counts}

    def comm_running(self, comm_id: str) -> bool:
        """Whether the communicator is in this report with every rank RUNNING."""
        statuses = self.comm_rank_status.get(comm_id)
        return bool(statuses) and all(
            rank_status == "RUNNING" for rank_status in statuses.values()
        )

    @cached_property
    def mismatched_comms(self) -> Set[str]:
        """Communicator ids whose ranks have op counts that are mismatched and its ranks are RUNNING."""
        return {
            comm_id
            for comm_id, op_skews in self.comm_op_skews.items()
            if any(skews > 0 for skews in op_skews.values())
            and self.comm_running(comm_id)
        }

    def mismatched_op_ranks(self, comm_id: str) -> Dict[str, Dict[int, List[int]]]:
        """Group a communicator's ranks by launch count, for each mismatched op.

        Args:
            comm_id: A communicator in this report.

        Returns:
            ``{op_name: {count: [rank, ...]}}`` for the ops whose counts
            differ between the communicator's ranks.
        """
        rank_op_counts = self.comm_op_counts[comm_id]
        mismatched: Dict[str, Dict[int, List[int]]] = {}
        for op, skew in self.op_skews(comm_id).items():
            if skew == 0:
                continue
            ranks_by_count: Dict[int, List[int]] = defaultdict(list)
            for rank, op_counts in rank_op_counts.items():
                ranks_by_count[op_counts.get(op, 0)].append(rank)
            mismatched[op] = ranks_by_count
        return mismatched

    @property
    def healthy(self) -> bool:
        """If the RAS reports missing ranks or mismatched communicators."""
        return not self.mismatched_comms

    @staticmethod
    def rank_status(status: Dict[str, Any]) -> str:
        """Convert a rank's status to a human-readable string."""
        # TODO: `async_error` is ignored currently in the upstream ras implementation
        #  https://github.com/NVIDIA/nccl/blob/master/src/ras/client_support.cc
        if status["abort_flag"] is True:
            return "ABORT"
        if status["finalize_called"] is True or status["destroy_flag"] is True:
            return "FINALIZE"
        if status["init_state"] == 0:
            return "RUNNING"
        else:
            return "INIT"


def compute_report_op_diff(
    prev: RASReport, curr: RASReport
) -> Dict[str, Dict[int, Dict[str, int]]]:
    """Per-communicator, per-rank op-count delta between two consecutive RAS reports.

    Args:
        prev: The previous poll's report.
        curr: The current poll's report.

    Returns:
        ``{comm_id: {rank: {op_name: cur - prev}}}``, restricted to the
        communicators and ranks the two reports share.
    """
    op_count_diff: Dict[str, Dict[int, Dict[str, int]]] = {}
    for comm_id, comm_curr_counts in curr.comm_op_counts.items():
        comm_prev_counts = prev.comm_op_counts.get(comm_id)
        if not comm_prev_counts:
            continue

        comm_diff: Dict[int, Dict[str, int]] = {}
        for rank, curr_counts in comm_curr_counts.items():
            if rank not in comm_prev_counts:
                continue

            prev_counts = comm_prev_counts[rank]
            comm_diff[rank] = {
                op: count - prev_counts.get(op, 0) for op, count in curr_counts.items()
            }

        op_count_diff[comm_id] = comm_diff
    return op_count_diff


# NCCL 2.28.9 emits malformed JSON for a ``missing_ranks[]`` with a missing comma
_MISSING_COMMA_RE = re.compile(r'([\d"el])(\s*\n\s*)("[^"\n]*"\s*:)')


def _parse_rank_processes(ranks: List[Dict[str, Any]]) -> Dict[int, Tuple[str, int]]:
    """The ``(host, pid)`` of each of a communicator's ranks that reports one.

    Only used to name ranks by Ray Train world rank, so a rank without a usable
    host or pid is left out rather than failing the whole report.

    Args:
        ranks: The communicator's ``ranks`` entries.

    Returns:
        ``{comm_rank: (host, pid)}`` for the ranks with both.
    """
    processes: Dict[int, Tuple[str, int]] = {}
    for rank in ranks:
        try:
            processes[rank["rank"]] = (str(rank["host"]), int(rank["pid"]))
        except (KeyError, TypeError, ValueError):
            continue
    return processes


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
        comm_op_counts, comm_rank_status, comm_rank_processes = {}, {}, {}
        for comm in data["communicators"]:
            comm_op_counts[comm["hash"]] = {
                rank["rank"]: {
                    op: int(count) for op, count in rank["collective_counts"].items()
                }
                for rank in comm["ranks"]
            }
            comm_rank_status[comm["hash"]] = {
                rank["rank"]: RASReport.rank_status(rank["status"])
                for rank in comm["ranks"]
            }
            comm_rank_processes[comm["hash"]] = _parse_rank_processes(comm["ranks"])

        return RASReport(
            timestamp=data["timestamp"],
            comm_op_counts=comm_op_counts,
            comm_rank_status=comm_rank_status,
            comm_rank_processes=comm_rank_processes,
            raw_json=ras_json,
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.info(
            "NCCL RAS JSON did not match the expected schema: %s",
            e,
        )
        return None


class RASQueryError(Exception):
    """A ``ncclras`` query produced no usable output.

    Args:
        reason: Short machine-readable cause, e.g. ``binary_not_found``,
            ``query_timeout``, ``exit_1``, ``unparseable_json``.
        message: Human-readable explanation; defaults to ``reason``.
        stderr: Tail of the binary's stderr, when there was one.
        fatal: True for run-wide misconfigurations no retry can fix (binary
            missing, binary too old for ``-f``); the poller stops on these.
    """

    def __init__(
        self,
        reason: str,
        message: Optional[str] = None,
        stderr: Optional[str] = None,
        fatal: bool = False,
    ):
        text = message or reason
        if stderr:
            text = f"{text} (stderr: {stderr.strip()})"
        super().__init__(text)
        self.reason = reason
        self.stderr = stderr
        self.fatal = fatal


class RASPoller:
    """Fetches RAS reports from the worker group on a background thread.

    Transport and threading only, no detection state. Vocabulary used here and
    in :class:`NCCLRASCallback`:

    - a *query* is one ``ncclras`` invocation on one worker;
    - a *poll* is one loop iteration: workers are queried in turn until one
      returns a usable report (every worker sees the same RAS mesh, so one
      success per poll is enough);
    - a *report* is the parsed :class:`RASReport`.

    The thread polls every ``interval_s`` and publishes each report to a queue
    the controller drains with :meth:`next_result`, one item per controller
    tick. A fatal query failure is published as the :class:`RASQueryError`
    itself and the thread exits, so every state change stays on the controller
    thread. :meth:`query` is also used synchronously by the callback for the
    human-readable ``-f text`` report at hang time.
    """

    def __init__(self, worker_group: WorkerGroup, interval_s: float):
        self._worker_group = worker_group
        self._interval_s = interval_s

        self._binary_path = "ncclras"
        # Until NCCL creates its first communicator there is no RAS service to
        # answer, so failed polls before the first report are expected.
        self._has_reported = False
        self._stop = threading.Event()
        self._results: "queue.SimpleQueue[Union[RASReport, RASQueryError]]" = (
            queue.SimpleQueue()
        )
        self._thread = threading.Thread(
            target=self._run, name="nccl-ras-poller", daemon=True
        )

    def start(self):
        self._thread.start()

    def stop(self):
        """Signal the thread to exit.

        Doesn't wait for an in-flight query to finish (up to
        ``_NCCL_RAS_QUERY_TIMEOUT_S`` per worker) so worker group teardown is
        never delayed; the thread is a daemon and exits on its own.
        """
        self._stop.set()

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def next_result(self) -> Optional[Union[RASReport, RASQueryError]]:
        """Take the oldest unread poll result, or ``None`` if there is none.

        Returns:
            A :class:`RASReport`, a fatal :class:`RASQueryError` (after which
            nothing more is published), or ``None``.
        """
        try:
            return self._results.get_nowait()
        except queue.Empty:
            return None

    def _run(self):
        """Thread body: poll, publish, wait out the interval, repeat."""
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self._results.put(self.query("json"))
                self._has_reported = True
            except RASQueryError as e:
                if e.fatal:
                    self._results.put(e)
                    return
                # A poll in flight at stop() fails as the workers shut down.
                if not self._stop.is_set():
                    logger.log(
                        logging.INFO if self._has_reported else logging.DEBUG,
                        "`ncclras` poll produced no report (%s). Will retry next poll.",
                        e,
                    )
            except Exception:  # noqa: BLE001
                if not self._stop.is_set():
                    logger.exception("Unexpected error polling `ncclras`. Will retry.")
            self._stop.wait(max(0.0, self._interval_s - (time.monotonic() - started)))

    def query(self, fmt: Literal["json", "text"]) -> Union[RASReport, str]:
        """Run ``ncclras`` on the first worker that answers.

        Args:
            fmt: ``json`` for a parsed :class:`RASReport`, ``text`` for the
                human-readable report.

        Returns:
            The parsed report (``json``) or raw text (``text``).

        Raises:
            RASQueryError: Every worker failed. The error is the last worker's;
                ``fatal`` is set when that failure was a misconfiguration.
        """
        workers = list(self._worker_group.get_workers())
        if not workers:
            raise RASQueryError("no_workers", "no workers available to query")

        last_error: Optional[RASQueryError] = None
        for worker in workers:
            try:
                return self._query_worker(worker, fmt)
            except RASQueryError as e:
                logger.debug(
                    "`ncclras` query failed on worker %s (%s). Trying the next worker.",
                    worker,
                    e,
                )
                last_error = e
        raise last_error

    def _query_worker(
        self, worker: Worker, fmt: Literal["json", "text"]
    ) -> Union[RASReport, str]:
        """Run a single ``ncclras`` query on one worker.

        Args:
            worker: The train worker to run ``ncclras`` on.
            fmt: ``json`` or ``text``.

        Returns:
            The parsed report (``json``) or raw text (``text``).

        Raises:
            RASQueryError: The query timed out, errored, exited non-zero, or
                produced output that could not be used.
        """
        try:
            ref = worker.execute_async(
                run_ncclras, self._binary_path, _NCCL_RAS_QUERY_TIMEOUT_S, fmt
            )
            result = ray.get(ref, timeout=_NCCL_RAS_QUERY_TIMEOUT_S)
        except GetTimeoutError:
            raise RASQueryError("query_timeout")
        except Exception as e:  # noqa: BLE001
            raise RASQueryError("query_error", str(e))

        if not result["ok"]:
            reason = result["reason"]
            if reason == "binary_not_found":
                raise RASQueryError(
                    reason,
                    f"binary {self._binary_path!r} not found on the worker.",
                    fatal=True,
                )
            if reason == "unsupported_f_option":
                raise RASQueryError(
                    reason,
                    f"binary {self._binary_path!r} rejected the `-f` format flag, "
                    "which requires NCCL 2.28+",
                    fatal=True,
                )
            raise RASQueryError(reason, stderr=result.get("stderr"))

        if fmt == "json":
            logger.debug("`ncclras` json output: %s", result["stdout"])
            report = parse_ras_schema(result["stdout"])
            if report is None:
                raise RASQueryError("unparseable_json")
            return report
        if not result["stdout"]:
            raise RASQueryError("empty_text_output")
        return result["stdout"]


@dataclass
class DiagnosticResult:
    """The results from a diagnostic (nvidia-smi, py-spy, etc) from a node or worker.

    Attributes:
        value: What the diagnostic function returned, or ``None`` if it didn't.
        error: The exception explaining why this diagnostic has no value, or
            ``None`` when it does.
    """

    value: Optional[Any] = None
    error: Optional[Exception] = None


def fan_out_to_workers(
    workers: List[Worker], fn: Callable[..., Any], *fn_args, timeout_s: float
) -> Dict[int, DiagnosticResult]:
    """Run ``fn`` on every worker in parallel and collect what each returned.

    Args:
        workers: The train workers to run ``fn`` on.
        fn: The worker-side function, called with ``fn_args``. It may return a
            ``DiagnosticResult`` to report its own failure, which is kept as is.
        *fn_args: Positional arguments forwarded to ``fn`` on every worker.
        timeout_s: Budget for the whole fan-out, shared by every worker.

    Returns:
        Dictionary of the diagnostic result with its world rank of the worker.
    """
    dumps: Dict[int, DiagnosticResult] = {}
    refs: Dict[ray.ObjectRef, int] = {}

    for worker in workers:
        rank = worker.distributed_context.world_rank
        try:
            refs[worker.execute_async(fn, *fn_args)] = rank
        except Exception as e:  # noqa: BLE001
            logger.info("Failed to launch %s on rank %d: %s", fn.__name__, rank, e)
            dumps[rank] = DiagnosticResult(error=e)

    if refs:
        _, not_ready = ray.wait(list(refs), num_returns=len(refs), timeout=timeout_s)
        for ref, rank in refs.items():
            if ref in not_ready:
                logger.warning(
                    "%s on rank %d did not finish within %.0fs. It will be missing "
                    "from the hang diagnostics.",
                    fn.__name__,
                    rank,
                    timeout_s,
                )
                dumps[rank] = DiagnosticResult(
                    error=TimeoutError(f"timed out after {timeout_s:.0f}s")
                )
                continue

            try:
                value = ray.get(ref)
                dumps[rank] = (
                    value
                    if isinstance(value, DiagnosticResult)
                    else DiagnosticResult(value=value)
                )
            except Exception as e:  # noqa: BLE001
                logger.info("Failed to collect %s on rank %d: %s", fn.__name__, rank, e)
                dumps[rank] = DiagnosticResult(error=e)

    return dumps


def dump_stack_trace(pyspy_timeout_s: float) -> str:
    """Dump native + Python stacks of the current (worker) process.

    Args:
        pyspy_timeout_s: Timeout for the ``py-spy dump`` subprocess.

    Returns:
        The captured stack trace, or a Python-only traceback (prefixed with the
        reason py-spy was skipped) when py-spy is unavailable.
    """
    pid = os.getpid()
    try:
        proc = subprocess.run(
            ["py-spy", "dump", "--pid", str(pid), "--native"],
            capture_output=True,
            text=True,
            timeout=pyspy_timeout_s,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout
        stderr = (proc.stderr or "").strip() or f"py-spy exited {proc.returncode}"
    except FileNotFoundError:
        stderr = "py-spy not installed"
    except subprocess.TimeoutExpired:
        stderr = "py-spy timed out"
    except Exception as e:  # noqa: BLE001
        stderr = f"py-spy error: {e}"

    # Python-only fallback: dump every thread's stack (cannot show C/C++ trace).
    import sys
    import traceback

    lines = [f"[py-spy unavailable: {stderr}; Python-only traceback follows]"]
    for thread_id, frame in sys._current_frames().items():
        lines.append(f"\n# Thread {thread_id}")
        lines.append("".join(traceback.format_stack(frame)))
    return "\n".join(lines)


def run_nvidia_smi(timeout_s: float) -> DiagnosticResult:
    """Snapshot `nvidia-smi -q` on the current (worker) node.

    Args:
        timeout_s: Timeout for the ``nvidia-smi`` subprocess.

    Returns:
        The report as ``value`` on success, otherwise ``error`` says why there
        is none.
    """
    try:
        proc = subprocess.run(
            ["nvidia-smi", "-q"], capture_output=True, text=True, timeout=timeout_s
        )
    except subprocess.TimeoutExpired:
        return DiagnosticResult(
            error=TimeoutError(
                f"`nvidia-smi -q` timed out after {timeout_s:.0f}s, which usually "
                "means the driver is itself stuck"
            )
        )
    except Exception as e:  # noqa: BLE001
        return DiagnosticResult(error=e)

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        return DiagnosticResult(
            error=RuntimeError(
                f"`nvidia-smi -q` exited {proc.returncode} (stderr: {stderr[:500]})"
            )
        )

    return DiagnosticResult(value=proc.stdout)


def translate_ras_ranks_to_train(
    workers: List[Worker], comm_rank_processes: Dict[int, Tuple[str, int]]
) -> Dict[int, int]:
    """Translate a communicator's ranks into Ray Train world ranks.

    RAS identifies a rank by the host and pid of its NCCL process, which for
    Ray Train is the worker actor's process. A pid is only unique per node, so
    ``(node_ip, pid)`` is matched first; the pid alone is the fallback when
    exactly one worker has it, because on multi-NIC nodes RAS can report a
    different interface's address than Ray's node IP.

    Args:
        workers: The train workers.
        comm_rank_processes: ``{comm_rank: (host, pid)}`` as reported by RAS.

    Returns:
        ``{comm_rank: train world rank}`` for the ranks with a matching train
        worker; ranks without one (e.g. a process the user spawned) are absent.
    """
    by_process: Dict[Tuple[str, int], int] = {}
    by_pid: Dict[int, List[int]] = defaultdict(list)
    for worker in workers:
        if worker.distributed_context is None:
            continue
        world_rank = worker.distributed_context.world_rank
        by_process[(worker.metadata.node_ip, worker.metadata.pid)] = world_rank
        by_pid[worker.metadata.pid].append(world_rank)

    train_rank_of: Dict[int, int] = {}
    for comm_rank, (host, pid) in comm_rank_processes.items():
        if (host, pid) in by_process:
            train_rank_of[comm_rank] = by_process[(host, pid)]
        elif len(by_pid.get(pid, [])) == 1:
            train_rank_of[comm_rank] = by_pid[pid][0]
    return train_rank_of


def format_ranks(comm_ranks: Iterable[int], train_rank_of: Dict[int, int]) -> str:
    """Name a communicator's ranks compactly, e.g. ``train ranks 0-3, 5``.

    Ranks are named by Ray Train world rank, falling back to the communicator
    rank for ranks without a train worker. Communicator ranks of the same
    worker (one process driving several GPUs) name its world rank once.
    Consecutive ranks collapse into
    ranges, and each list is cut after ``_MAX_RANKS_LOGGED`` runs so a
    1000-rank job stays one line.

    Args:
        comm_ranks: Communicator ranks to name.
        train_rank_of: The translation from :func:`match_train_ranks`.

    Returns:
        The train ranks, then the unmatched communicator ranks.
    """
    comm_ranks = list(comm_ranks)
    groups = [
        ("train rank", {train_rank_of[r] for r in comm_ranks if r in train_rank_of}),
        ("comm rank", {r for r in comm_ranks if r not in train_rank_of}),
    ]
    parts = []
    for noun, ranks in groups:
        if not ranks:
            continue
        runs: List[List[int]] = []
        for rank in sorted(ranks):
            if runs and runs[-1][1] == rank - 1:
                runs[-1][1] = rank
            else:
                runs.append([rank, rank])
        shown = [
            str(lo) if lo == hi else f"{lo}-{hi}" for lo, hi in runs[:_MAX_RANKS_LOGGED]
        ]
        hidden = sum(hi - lo + 1 for lo, hi in runs[_MAX_RANKS_LOGGED:])
        if hidden:
            shown.append(f"and {hidden} more")
        plural = "s" if len(ranks) > 1 else ""
        parts.append(f"{noun}{plural} {', '.join(shown)}")
    if parts and groups[1][1]:
        parts[-1] += " (no matching train worker)"
    return ", ".join(parts)


def dump_flight_recorder() -> DiagnosticResult:
    """Dump the PyTorch Flight Recorder buffer of the current (worker) process.

    The buffer has to be armed *before* the process group is created (see
    ``TORCH_FR_BUFFER_SIZE``); it cannot be switched on retroactively in a hung
    process, in which case the dump holds no collectives.

    Returns:
        The dump as a JSON string, or the error saying why there is no dump,
        which is written into the rank's file so a gap is never silent.
    """
    try:
        from torch._C import _distributed_c10d as c10d
    except ImportError as e:
        return DiagnosticResult(error=e)

    try:
        # The default dumps every collective on this rank (no timeout available on torch side)
        trace_bytes = c10d._dump_fr_trace_json()
        trace_json = trace_bytes.decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        return DiagnosticResult(error=e)

    return DiagnosticResult(value=trace_json)


class NCCLRASCallback(WorkerGroupCallback, ControllerCallback):
    """Detects NCCL hangs via the RAS subsystem (see module docstring for the
    topology and the hard/soft model).

    Default-off: the trainer only registers this callback when
    ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1`` is set on the driver.

    To confirm that a NCCL anomaly isn't a single-snapshot blip, a communicator
    must stay frozen for consecutive polls, tracked as a per-communicator
    frozen-poll streak (so each communicator is confirmed on its own) and
    reset by any healthy poll. ``RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S``
    expresses how long that run should take and is converted to a poll count
    with the poll interval.

    Every poll is added to a circular buffer so users have a history of ncclras
    queries and for improving the detection.

    Every poll whose outcome the detector logs (a communicator newly or still
    suspected, confirmed, or recovered), and the detector stopping while a
    communicator is stalled, is exported through the Train state actor as an
    ``EXPORT_TRAIN_NCCL_HANG`` event: a snapshot of every non-healthy
    communicator, so platforms can show the detector's state without parsing
    logs. Exporting never affects detection.

    The run-level detector state (healthy, suspected, confirmed) and the
    longest stall are also recorded as metrics on every poll, for a time series
    of the run.
    """

    def __init__(self):
        self._poll_interval_s = env_float(
            NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, DEFAULT_NCCL_RAS_MIN_POLL_INTERVAL_S
        )
        if self._poll_interval_s <= 0:
            raise ValueError(
                f"{NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR} must be a positive number "
                f"of seconds, got {self._poll_interval_s}."
            )
        self._confirm_duration_s = env_float(
            NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR, DEFAULT_NCCL_RAS_CONFIRM_DURATION_S
        )
        if self._confirm_duration_s <= 0:
            raise ValueError(
                f"{NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR} must be a positive number "
                f"of seconds, got {self._confirm_duration_s}."
            )

        # Escalation milestones, in polls
        self._confirm_poll_counts = math.ceil(
            self._confirm_duration_s / self._poll_interval_s
        )
        self._suspicion_polls = min(
            math.ceil(_FIRST_SUSPICION_AFTER_S / self._poll_interval_s),
            self._confirm_poll_counts - 1,
        )
        self._periodic_warn_polls = math.ceil(
            _PERIODIC_WARN_EVERY_S / self._poll_interval_s
        )

        self._action = os.environ.get(
            NCCL_RAS_ACTION_ENV_VAR, DEFAULT_NCCL_RAS_ACTION
        ).lower()
        if self._action not in (NCCL_RAS_ACTION_FAIL, NCCL_RAS_ACTION_OBSERVE):
            raise ValueError(
                f"{NCCL_RAS_ACTION_ENV_VAR} must be one of "
                f"{NCCL_RAS_ACTION_FAIL!r} or {NCCL_RAS_ACTION_OBSERVE!r}, "
                f"got {self._action!r}."
            )

        self._worker_group: Optional[WorkerGroup] = None
        self._ras_poller: Optional[RASPoller] = None

        # The previous successful poll's report
        self.prev_report: Optional[RASReport] = None
        # Circular buffer of ncclras json queries
        self.ras_history: Deque[RASReport] = deque(
            maxlen=self._confirm_poll_counts + _RAS_HISTORY_MARGIN_POLLS
        )
        # Per-communicator consecutive frozen-poll streaks ({comm_id: polls}).
        # As a deadlock requires the whole comm to be frozen (no op advancing),
        # any op progressing would indicate the comm overall isn't deadlocked.
        self.comm_deadlock_count: Dict[str, int] = {}

        # Whether the `ncclras` text report was logged since the latest new
        # suspicion, so it's logged once per suspicion rather than on every
        # warning, and retried on the next warning if the fetch failed.
        self._ras_text_logged: bool = False

        # One-time degradation (e.g. missing binary) so we stop querying.
        self._is_ras_degraded: bool = False

        # Identify the exported hang events, set when a worker group starts.
        self._run_id: str = ""
        self._attempt_id: str = ""
        # Polls (reports) of the current attempt, numbering the exported events.
        self._poll_index: int = 0
        # When the latest and the previous reports were taken from the poller,
        # in ms since epoch.
        self._poll_time_ms: int = 0
        self._prev_poll_time_ms: int = 0
        # Per-communicator state of the current stall episode, for the exported
        # events: when its stall began, and whether it was confirmed (it is
        # then reported as confirmed for the rest of the episode).
        self._stalled_since_ms: Dict[str, int] = {}
        self._confirmed_comms: Set[str] = set()
        # Whether the STOPPED event of the current attempt was exported.
        self._stop_exported: bool = False
        self._exporter = NcclHangEventExporter()

        # The latest report's `launch_groups` per communicator, as both the
        # log line and the exported event read them.
        self._launch_groups_report: Optional[RASReport] = None
        self._launch_groups_cache: Dict[
            str, Tuple[Dict[int, int], List[Tuple[str, int, List[int]]]]
        ] = {}

        # Run-level state gauges (see `NcclHangDetectorMetrics`), shared by
        # every attempt, or None if they couldn't be created yet.
        self._metrics: Optional[Dict[str, GaugeMetric]] = None

    def reset_detection_state(self):
        """Full worker-group lifecycle reset (on (re)start / shutdown)."""
        self.prev_report = None
        self.ras_history.clear()
        self.reset_hang_counters()

    def reset_hang_counters(self):
        """Per-healthy-poll debounce reset `_prev_report` left for the next poll as comparison."""
        self.comm_deadlock_count = {}
        self._stalled_since_ms = {}
        self._confirmed_comms = set()

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._worker_group = worker_group
        self.reset_detection_state()
        self._poll_index = 0
        self._stop_exported = False
        try:
            self._run_id = worker_group._train_run_context.run_id
            self._attempt_id = worker_group.get_worker_group_context().run_attempt_id
        except Exception:  # noqa: BLE001
            logger.debug("Could not read the run and attempt ids.", exc_info=True)
        self._retire_poller()
        if self._is_ras_degraded:
            return
        if self._metrics is None:
            try:
                self._metrics = NcclHangDetectorMetrics.get_nccl_hang_detector_metrics(
                    worker_group._train_run_context.get_run_config().name,
                    self._run_id,
                )
            except Exception:  # noqa: BLE001
                logger.debug(
                    "Could not create the hang detector metrics.", exc_info=True
                )
        self._ras_poller = RASPoller(worker_group, self._poll_interval_s)
        self._ras_poller.start()

    def before_worker_group_shutdown(self, worker_group):
        self._retire_poller()
        # Shutdown always precedes a restart, which starts a fresh attempt.
        self.stop_watching(NcclHangStopReason.WORKER_GROUP_SHUTDOWN)
        self._worker_group = None

    def _retire_poller(self):
        if self._ras_poller is not None:
            self._ras_poller.stop()
            self._ras_poller = None

    def after_worker_group_poll_status(self, worker_group_status):
        if self._is_ras_degraded or self._ras_poller is None:
            return

        # This hook runs on the controller's poll loop, so any error here must
        # never crash training.
        try:
            result = self._ras_poller.next_result()
            if result is None:
                # The poller hasn't completed a new poll since the last tick
                return
            if isinstance(result, RASQueryError):
                logger.warning(
                    "Disabling Ray Train hang detector for the rest of this run as `ncclras` returns %s.",
                    result,
                )
                self._is_ras_degraded = True
                self.stop_watching(NcclHangStopReason.DETECTOR_DISABLED)
                return

            self._poll_index += 1
            self._prev_poll_time_ms = self._poll_time_ms
            self._poll_time_ms = int(time.time() * 1000)
            self.ras_history.append(result)

            # A healthy report still ends (or holds) any open streak.
            if result.mismatched_comms or self.comm_deadlock_count:
                self.evaluate_comm_mismatch(result)
            self.prev_report = result
            self.record_state_metrics()
        except NCCLHangError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception(
                "The Ray Train hang detector hit an unexpected error, therefore, "
                "disabling it for the rest of this training run."
            )
            self._is_ras_degraded = True
            self.stop_watching(NcclHangStopReason.DETECTOR_DISABLED)

    def evaluate_comm_mismatch(self, report: RASReport):
        """Advance the per-communicator frozen streaks and act on them.

        A communicator is deadlocked only when *no* rank advanced *any* op since
        the last poll: a real hang blocks every rank, so every op freezes. It's
        possible for an op mismatch to occur and NCCL continue which isn't
        detected currently.
        """
        if self.prev_report is None:
            return

        # 1. Classify: which mismatched communicators made no progress this
        # poll, and which stalled ones can no longer be observed
        frozen_counts = self.compute_frozen_streaks(report)
        held_counts = self.held_streaks(report, frozen_counts)
        stalled_counts = {**held_counts, **frozen_counts}
        confirmed_comm_hangs = [
            comm_id
            for comm_id, count in frozen_counts.items()
            if count == self._confirm_poll_counts
        ]

        # 2. Update state: a communicator that progressed drops its streak
        recovered, recovered_messages = self.log_recovered_comms(stalled_counts)
        self.comm_deadlock_count = stalled_counts
        # A new streak's counts were already frozen at the previous poll, so
        # the stall is dated from it.
        self._stalled_since_ms = {
            comm_id: self._stalled_since_ms.get(comm_id, self._prev_poll_time_ms)
            for comm_id in stalled_counts
        }
        self._confirmed_comms.intersection_update(stalled_counts)

        # 3. Act, reporting everything this poll found in one exported event
        triggers: List[NcclHangTrigger] = []
        messages: List[str] = []
        ras_text_report: Optional[str] = None
        diagnostics_dir: Optional[str] = None
        hang_message: Optional[str] = None
        if confirmed_comm_hangs:
            (
                headline,
                hang_message,
                ras_text_report,
                diagnostics_dir,
            ) = self.capture_confirmed_hangs(confirmed_comm_hangs, report)
            triggers.append(NcclHangTrigger.CONFIRMED)
            messages.append(headline)
            # The other communicators' milestones on the same poll are still
            # reported, with the report fetched for the confirmation.
            suspected = self.handle_suspected_hangs(
                report,
                frozen_counts,
                confirmed=confirmed_comm_hangs,
                ras_text_report=ras_text_report,
            )
        elif frozen_counts:
            suspected = self.handle_suspected_hangs(report, frozen_counts)
        else:
            suspected = None
        if suspected is not None:
            headline, fetched_report = suspected
            triggers.append(NcclHangTrigger.SUSPECTED)
            messages.append(headline)
            ras_text_report = ras_text_report or fetched_report
        if recovered:
            triggers.append(NcclHangTrigger.RECOVERED)
            messages.extend(recovered_messages)

        if triggers:
            # Exported before the action so the event exists even when it raises.
            self.export_poll(
                report,
                triggers,
                messages,
                ras_text_report=ras_text_report,
                diagnostics_dir=diagnostics_dir,
                recovered=recovered,
            )
        if hang_message is not None:
            # Before the action, as a fail-mode raise skips the end of the poll.
            self.record_state_metrics()
            if self._action == NCCL_RAS_ACTION_FAIL:
                raise NCCLHangError(hang_message, worker_failures={})
            elif self._action == NCCL_RAS_ACTION_OBSERVE:
                logger.warning(hang_message)

    def compute_frozen_streaks(self, report: RASReport) -> Dict[str, int]:
        """Extend the frozen streak of every mismatched communicator that stalled.

        Args:
            report: The current poll's report, diffed against ``prev_report``.

        Returns:
            ``{comm_id: consecutive frozen polls}`` for the communicators that
            are mismatched in ``report`` and whose ranks all advanced zero ops
            since ``prev_report``. Communicators that progressed, or are new
            this poll, are absent so their streak restarts from zero.
        """
        op_diff = compute_report_op_diff(self.prev_report, report)

        frozen_counts: Dict[str, int] = {}
        for comm_id in report.mismatched_comms:
            if comm_id not in op_diff:
                continue
            comm_frozen = all(
                delta == 0
                for op_deltas in op_diff[comm_id].values()
                for delta in op_deltas.values()
            )
            if comm_frozen:
                frozen_counts[comm_id] = self.comm_deadlock_count.get(comm_id, 0) + 1
        return frozen_counts

    def held_streaks(
        self, report: RASReport, frozen_counts: Dict[str, int]
    ) -> Dict[str, int]:
        """Keep the streaks of stalled communicators that can't be observed.

        A communicator stops being mismatched when its counts move, but also
        when a rank leaves RUNNING (aborted or finalized, e.g. a rank that
        crashed or exited early) or it drops out of the report. That is no
        evidence of progress, so it isn't a recovery: the streak is held at
        its count, neither advancing (no confirmation) nor warning again,
        until progress is seen or the detector stops watching the attempt.
        Streaks below the suspicion threshold were never reported, so they are
        dropped as before, e.g. a process group destroyed mid-streak.

        Args:
            report: The current poll's report.
            frozen_counts: This poll's advanced streaks.

        Returns:
            ``{comm_id: held count}`` for the suspected streaks that aren't in
            ``frozen_counts`` and whose communicator isn't running.
        """
        return {
            comm_id: count
            for comm_id, count in self.comm_deadlock_count.items()
            if count >= self._suspicion_polls
            and comm_id not in frozen_counts
            and not report.comm_running(comm_id)
        }

    def log_recovered_comms(
        self, frozen_counts: Dict[str, int]
    ) -> Tuple[List[Tuple[str, int, float]], List[str]]:
        """Log each previously suspected communicator that is no longer frozen.

        Args:
            frozen_counts: This poll's streaks; anything in
                ``comm_deadlock_count`` but not here has resumed progress.

        Returns:
            ``[(comm_id, stalled_since_ms, final stall duration in seconds)]``
            for the recovered communicators, and their log messages.
        """
        recovered: List[Tuple[str, int, float]] = []
        messages: List[str] = []
        for comm_id, count in self.comm_deadlock_count.items():
            if comm_id not in frozen_counts and count >= self._suspicion_polls:
                message = (
                    "The Ray Train hang detector callback detects that the "
                    f"NCCL communicator {comm_id} resumed making progress after "
                    f"being stalled for {count * self._poll_interval_s:.0f} seconds "
                    f"({count} polls). It is no longer suspected of hanging."
                )
                logger.info(message)
                messages.append(message)
                # It was last seen frozen at the previous poll.
                stalled_since_ms = self._stalled_since_ms.get(
                    comm_id,
                    self._prev_poll_time_ms - int(count * self._poll_interval_s * 1000),
                )
                recovered.append(
                    (
                        comm_id,
                        stalled_since_ms,
                        (self._prev_poll_time_ms - stalled_since_ms) / 1000,
                    )
                )
        return recovered, messages

    def stop_watching(self, reason: NcclHangStopReason):
        """Drop the stall episodes, exporting STOPPED if any was suspected.

        Never raises, as it runs on the paths that disable the detector.

        Args:
            reason: Why the detector stops watching the attempt.
        """
        try:
            hang_confirmed = bool(self._confirmed_comms)
            self.export_stopped(reason)
            self.reset_hang_counters()
            # The state is unknown from here on, so the gauges stop reporting
            # a hang rather than keep the last one forever. A confirmed hang
            # the attempt is shut down with is kept, as the attempt's outcome,
            # until the next attempt's first report: in fail mode the shutdown
            # it raises follows within milliseconds, before any scrape.
            keep_confirmed = (
                hang_confirmed and reason == NcclHangStopReason.WORKER_GROUP_SHUTDOWN
            )
            if self._metrics is not None and not keep_confirmed:
                for metric in self._metrics.values():
                    metric.reset()
        except Exception:  # noqa: BLE001
            logger.debug("Could not drop the stalled communicators.", exc_info=True)

    def record_state_metrics(self):
        """Set the run-level gauges from the worst open stall episode.

        Never raises: metrics must not affect detection.
        """
        if self._metrics is None:
            return
        try:
            open_counts = {
                comm_id: count
                for comm_id, count in self.comm_deadlock_count.items()
                if count >= self._suspicion_polls
            }
            if self._confirmed_comms:
                state = NcclHangDetectorState.CONFIRMED
            elif open_counts:
                state = NcclHangDetectorState.SUSPECTED
            else:
                state = NcclHangDetectorState.HEALTHY
            stall_s = max(open_counts.values(), default=0) * self._poll_interval_s
            self._metrics[NcclHangDetectorMetrics.DETECTOR_STATE].record(float(state))
            self._metrics[NcclHangDetectorMetrics.STALL_DURATION_S].record(stall_s)
        except Exception:  # noqa: BLE001
            logger.debug("Could not record the hang detector metrics.", exc_info=True)

    def capture_confirmed_hangs(
        self, confirmed_comm_hangs: List[str], report: RASReport
    ) -> Tuple[str, str, Optional[str], Optional[str]]:
        """Capture the diagnostics of confirmed hangs and describe them.

        Args:
            confirmed_comm_hangs: The communicators confirmed on this poll.
            report: The current poll's report.

        Returns:
            The log headline, the full message (logged or raised by the
            action), the ``ncclras -f text`` report fetched for the diagnostics
            and the diagnostics directory (``None`` if nothing was captured).
        """
        # Record first to prevent buffer being overwritten by other ranks
        flight_recorder_dir = self.capture_diagnostic(
            "Flight Recorder dumps", self.dump_workers_flight_recorder
        )
        nvidia_smi_dir = self.capture_diagnostic(
            "nvidia-smi snapshots", self.dump_nodes_nvidia_smi
        )
        # Fetched once for both the saved history and the exported event.
        ras_human_report: Optional[str] = None

        def dump_ras_history() -> Optional[str]:
            nonlocal ras_human_report
            ras_human_report = self.fetch_ras_human_report()
            return self.dump_ras_query_history(ras_human_report)

        ras_history_dir = self.capture_diagnostic(
            "`ncclras` query history", dump_ras_history
        )
        stack_trace_dir = self.capture_diagnostic(
            "worker stack traces", self.dump_workers_stack_traces
        )

        headline = (
            "NCCL hang detected by the Ray Train hang detector callback! "
            f"{len(confirmed_comm_hangs)} communicator(s) made no collective "
            f"progress for {self._confirm_duration_s:.0f} seconds "
            f"({self._confirm_poll_counts} polls) while their ranks disagree on "
            "how many collectives they have launched."
        )
        message = (
            f"{headline}\n"
            f"{self.describe_stalled_comms(report, confirmed_comm_hangs)}\n"
            "Common causes are a rank taking a divergent code path or exiting "
            "early, ranks launching collectives in a different order or with "
            "mismatched shapes or dtypes, or a GPU or network failure.\n"
        )
        debug_steps = []
        if stack_trace_dir:
            debug_steps.append(
                "Compare the stack traces of the ranks with different launch "
                f"counts: {stack_trace_dir}"
            )
        if ras_history_dir:
            debug_steps.append(
                "Read NCCL's own report (ncclras_report.txt) and how the launch "
                f"counts drifted over the polls before the hang: {ras_history_dir}"
            )
        if nvidia_smi_dir:
            debug_steps.append(
                "Rule out hardware issues with the per-node `nvidia-smi` "
                "snapshots of every GPU's power, temperature, clocks and ECC "
                f"state at the moment of the hang: {nvidia_smi_dir}"
            )
        if flight_recorder_dir:
            debug_steps.append(
                "Inspect the collective in flight on each rank, with its op "
                "type, shapes and call stack, in the per-rank PyTorch Flight "
                f"Recorder dumps: {flight_recorder_dir}"
            )
        if debug_steps:
            message += "To debug:\n" + "".join(f"  - {step}\n" for step in debug_steps)

        self._confirmed_comms.update(confirmed_comm_hangs)
        diagnostics_dir = (
            self.diagnostics_dir()
            if any(
                (flight_recorder_dir, nvidia_smi_dir, ras_history_dir, stack_trace_dir)
            )
            else None
        )
        return headline, message, ras_human_report or None, diagnostics_dir

    def handle_suspected_hangs(
        self,
        report: RASReport,
        frozen_counts: Dict[str, int],
        confirmed: Sequence[str] = (),
        ras_text_report: Optional[str] = None,
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Warn about suspected hangs at the suspicion and periodic milestones.

        Args:
            report: The current poll's report.
            frozen_counts: The streaks that advanced this poll. Only these
                reach a milestone; held streaks don't warn again.
            confirmed: The communicators confirmed on this poll, which are left
                out, as the confirmation describes them. If any, the warning
                uses ``ras_text_report`` instead of fetching another report.
            ras_text_report: The report already fetched this poll, if
                ``confirmed``.

        Returns:
            ``None`` if nothing was warned about, otherwise the warning's
            headline and the ``ncclras -f text`` report fetched for it, if any.
        """
        # A communicator first warns at the suspicion threshold, then every
        # `_periodic_warn_polls` after it.
        advanced = {
            comm_id: count
            for comm_id, count in frozen_counts.items()
            if comm_id not in confirmed
        }
        new_suspicions = [
            comm_id
            for comm_id, count in advanced.items()
            if count == self._suspicion_polls
        ]
        if new_suspicions:
            # An earlier report predates these communicators stalling.
            self._ras_text_logged = False
            stalled_comms = new_suspicions
            headline = (
                f"Possible NCCL hang detected by the Ray Train hang detector callback! "
                f"{len(new_suspicions)} communicator(s) have made no collective progress "
                f"for {self._suspicion_polls * self._poll_interval_s:.0f} seconds "
                "while their ranks disagree on how many collectives they have "
                "launched. Continuing to monitor, this might be a transient stall."
            )
        elif any(
            count > self._suspicion_polls
            and (count - self._suspicion_polls) % self._periodic_warn_polls == 0
            for count in advanced.values()
        ):
            # Every open episode, as the exported event lists them.
            stalled_comms = [
                comm_id
                for comm_id, count in self.comm_deadlock_count.items()
                if count >= self._suspicion_polls and comm_id not in confirmed
            ]
            headline = (
                f"NCCL hang still suspected! {len(stalled_comms)} "
                "communicator(s) have made no progress."
            )
        else:
            return None

        if self._action == NCCL_RAS_ACTION_FAIL and not confirmed:
            remaining_polls = self._confirm_poll_counts - max(advanced.values())
            headline += (
                " A NCCLHangError will be raised in "
                f"{remaining_polls * self._poll_interval_s:.0f} seconds "
                f"({remaining_polls} more polls) if this persists."
            )
        logger.warning(
            "%s\n%s", headline, self.describe_stalled_comms(report, stalled_comms)
        )

        # NCCL's own view of the hang, once per suspected hang; it is saved
        # with the diagnostics if the hang is confirmed.
        ras_human_output: Optional[str] = None
        if not self._ras_text_logged:
            ras_human_output = (
                ras_text_report if confirmed else self.fetch_ras_human_report()
            )
            if ras_human_output:
                logger.info(
                    "NCCL's own `ncclras` report of the suspected hang (logged "
                    "once per hang). Its ranks are numbered per communicator, "
                    "not by Ray Train world rank:\n%s",
                    ras_human_output.rstrip(),
                )
                self._ras_text_logged = True
        return headline, ras_human_output or None

    def describe_stalled_comms(
        self, report: RASReport, stalled_comm_ids: List[str]
    ) -> str:
        """One line per communicator with its launch counts by train rank.

        NCCL numbers a communicator's ranks from 0, which differs from the
        Ray Train world rank on any communicator but the world one, so every
        rank is translated, e.g. ``AllReduce launches: 622 by train rank 2;
        621 by train rank 3``. Only the collectives whose counts disagree are
        listed.

        Never raises: a failure here only loses log detail, so it must not
        reach the poll hook and disable detection.

        Args:
            report: The current poll's report.
            stalled_comm_ids: The stalled communicators to describe.

        Returns:
            One line per communicator, or ``""`` if they couldn't be built.
        """

        try:
            described_stalls = []
            for comm_id in stalled_comm_ids:
                stalled_s = (
                    self.comm_deadlock_count.get(comm_id, 0) * self._poll_interval_s
                )
                if comm_id not in report.comm_op_counts:
                    described_stalls.append(
                        f"  - Communicator {comm_id}, no progress for "
                        f"{stalled_s:.0f}s before it left the `ncclras` report."
                    )
                    continue
                train_rank_of, launch_groups = self.launch_groups(report, comm_id)
                ranks_by_op: Dict[str, List[str]] = defaultdict(list)
                for op, count, ranks in launch_groups:
                    ranks_by_op[op].append(
                        f"{count} by {format_ranks(ranks, train_rank_of)}"
                    )
                ops = [
                    f"{op} launches: " + "; ".join(counts)
                    for op, counts in ranks_by_op.items()
                ]
                comm_ranks = format_ranks(report.comm_op_counts[comm_id], train_rank_of)
                described_stalls.append(
                    f"  - Communicator {comm_id} over {comm_ranks}, "
                    f"no progress for {stalled_s:.0f}s. {'. '.join(ops)}"
                )
            return "\n".join(described_stalls)

        except Exception:  # noqa: BLE001
            logger.debug("Could not describe the stalled communicators.", exc_info=True)
            return ""

    def launch_groups(
        self, report: RASReport, comm_id: str
    ) -> Tuple[Dict[int, int], List[Tuple[str, int, List[int]]]]:
        """Group a communicator's ranks by launch count, for each mismatched op.

        Shared by the log line and the exported stalled ranks, so they agree,
        and computed once per communicator of the latest report.

        Args:
            report: The current poll's report.
            comm_id: A mismatched communicator in ``report``.

        Returns:
            The ``{comm_rank: train world rank}`` translation and
            ``[(op, launch_count, comm_ranks)]``, ordered by op, then by
            launch count from highest to lowest.
        """
        if report is not self._launch_groups_report:
            self._launch_groups_report = report
            self._launch_groups_cache = {}
        if comm_id not in self._launch_groups_cache:
            workers = self._worker_group.get_workers() if self._worker_group else []
            train_rank_of = translate_ras_ranks_to_train(
                workers, report.comm_rank_processes.get(comm_id, {})
            )
            groups = [
                (op, count, ranks)
                for op, ranks_by_count in sorted(
                    report.mismatched_op_ranks(comm_id).items()
                )
                for count, ranks in sorted(ranks_by_count.items(), reverse=True)
            ]
            self._launch_groups_cache[comm_id] = (train_rank_of, groups)
        return self._launch_groups_cache[comm_id]

    def stalled_ranks(self, report: RASReport, comm_id: str) -> List[StalledRanks]:
        """The communicator's Ray Train world ranks grouped by launch count.

        Never raises: a failure here only loses event detail.

        Args:
            report: The current poll's report.
            comm_id: A mismatched communicator in ``report``.

        Returns:
            One entry per (op, launch count) the ranks disagree on. Ranks
            without a matching train worker are left out. ``[]`` if they
            couldn't be built.
        """
        try:
            train_rank_of, launch_groups = self.launch_groups(report, comm_id)
            return [
                StalledRanks(
                    op=op,
                    launch_count=count,
                    world_ranks=sorted(
                        {train_rank_of[rank] for rank in ranks if rank in train_rank_of}
                    ),
                )
                for op, count, ranks in launch_groups
            ]
        except Exception:  # noqa: BLE001
            logger.debug("Could not group the stalled ranks.", exc_info=True)
            return []

    def _open_comm_states(
        self, report: Optional[RASReport], now_ms: int
    ) -> List[NcclCommunicatorState]:
        """The state of every communicator past the suspicion threshold.

        Args:
            report: The report to take the stalled ranks from, if any.
            now_ms: The time the stall durations are measured to.

        Returns:
            One state per open communicator, ordered by communicator id.
        """
        states = []
        for comm_id, count in sorted(self.comm_deadlock_count.items()):
            if count < self._suspicion_polls:
                continue
            stalled_since_ms = self._stalled_since_ms.get(
                comm_id, now_ms - int(count * self._poll_interval_s * 1000)
            )
            states.append(
                NcclCommunicatorState(
                    communicator_id=comm_id,
                    state=(
                        NcclCommunicatorHangState.CONFIRMED
                        if comm_id in self._confirmed_comms
                        else NcclCommunicatorHangState.SUSPECTED
                    ),
                    stalled_since_ms=stalled_since_ms,
                    stall_duration_s=(now_ms - stalled_since_ms) / 1000,
                    stalled_ranks=(
                        self.stalled_ranks(report, comm_id)
                        if report and comm_id in report.comm_op_counts
                        else []
                    ),
                )
            )
        return states

    def export_poll(
        self,
        report: RASReport,
        triggers: List[NcclHangTrigger],
        messages: List[str],
        ras_text_report: Optional[str] = None,
        diagnostics_dir: Optional[str] = None,
        recovered: Iterable[Tuple[str, int, float]] = (),
    ):
        """Export a snapshot of the detector at a poll it reported on.

        Never raises, so exporting can never affect detection.

        Args:
            report: The current poll's report.
            triggers: What the poll reported.
            messages: The poll's log headlines.
            ras_text_report: The ``ncclras -f text`` report fetched this poll.
            diagnostics_dir: Where the diagnostics were uploaded this poll.
            recovered: ``(comm_id, stalled_since_ms, final duration)`` of the
                communicators that recovered this poll.
        """
        try:
            communicators = self._open_comm_states(report, self._poll_time_ms)
            communicators += [
                NcclCommunicatorState(
                    communicator_id=comm_id,
                    state=NcclCommunicatorHangState.RECOVERED,
                    stalled_since_ms=stalled_since_ms,
                    stall_duration_s=stall_duration_s,
                )
                for comm_id, stalled_since_ms, stall_duration_s in sorted(recovered)
            ]
            self._emit(
                TrainNcclHangEvent(
                    run_id=self._run_id,
                    attempt_id=self._attempt_id,
                    poll_index=self._poll_index,
                    timestamp_ms=self._poll_time_ms,
                    detector_status=NcclHangDetectorStatus.ACTIVE,
                    triggers=triggers,
                    message="\n".join(messages),
                    ras_text_report=ras_text_report,
                    diagnostics_dir=diagnostics_dir,
                    communicators=communicators,
                )
            )
        except Exception:  # noqa: BLE001
            logger.debug(
                "Could not export NCCL hang poll %d.", self._poll_index, exc_info=True
            )

    def export_stopped(self, reason: NcclHangStopReason):
        """Export that the detector stopped watching the attempt, once, if any
        communicator was past the suspicion threshold.

        Never raises, so exporting can never affect detection.

        Args:
            reason: Why the detector stopped.
        """
        try:
            now_ms = int(time.time() * 1000)
            communicators = (
                []
                if self._stop_exported
                else self._open_comm_states(self.prev_report, now_ms)
            )
            if communicators:
                self._stop_exported = True
                self._emit(
                    TrainNcclHangEvent(
                        run_id=self._run_id,
                        attempt_id=self._attempt_id,
                        poll_index=self._poll_index + 1,
                        timestamp_ms=now_ms,
                        detector_status=NcclHangDetectorStatus.STOPPED,
                        stop_reason=reason,
                        message=(
                            "The Ray Train hang detector stopped watching this "
                            f"attempt ({reason.value}) with {len(communicators)} "
                            "NCCL communicator(s) still stalled."
                        ),
                        communicators=communicators,
                    )
                )
        except Exception:  # noqa: BLE001
            logger.debug("Could not export that the detector stopped.", exc_info=True)

    def _emit(self, event: TrainNcclHangEvent):
        """Hand an event to the Train state actor, fire-and-forget."""
        self._exporter.export(event)

    def diagnostics_dir(self) -> Optional[str]:
        """Where the diagnostics of a confirmed hang are uploaded, if known."""
        try:
            return os.path.join(
                self._worker_group._storage_context.experiment_fs_path,
                HANG_DETECTOR_DIRNAME,
            )
        except Exception:  # noqa: BLE001
            return None

    def fetch_ras_human_report(self) -> Optional[str]:
        """Synchronously fetch ``ncclras -f text`` for the logs.

        Runs on the controller thread; only called once a hang is suspected or
        confirmed, when the job is already stalled.

        Returns:
            The report, or ``None`` if no worker could produce one.
        """
        try:
            return self._ras_poller.query("text")
        except RASQueryError as e:
            logger.info("Could not fetch the `ncclras` text report (%s).", e)
            return None

    @staticmethod
    def capture_diagnostic(
        name: str, capture: Callable[[], Optional[str]]
    ) -> Optional[str]:
        """Run one diagnostic capture, logging rather than raising on failure.

        Args:
            name: What is being captured, for the log message.
            capture: The capture, returning where it was uploaded.

        Returns:
            Where the diagnostic was uploaded, or ``None`` if it failed.
        """
        try:
            return capture()
        except Exception:  # noqa: BLE001
            logger.exception("Trying to capture the %s failed.", name)
            return None

    def dump_ras_query_history(
        self, human_report: Optional[str] = None
    ) -> Optional[str]:
        """Write the retained RAS polls to the run's storage.

        Args:
            human_report: The ``ncclras -f text`` report fetched at confirmation,
                or ``None`` if no worker could produce one.

        Returns:
            The path to the folder with the history, or ``None`` if no poll has
            been recorded yet.
        """
        files = {
            f"ncclras_{_UNSAFE_FILENAME_CHARS.sub('-', report.timestamp)}.json": report.raw_json
            for report in self.ras_history
        }
        if human_report:
            files["ncclras_report.txt"] = human_report

        if files:
            return self.upload_diagnostics(_NCCL_RAS_TOOL, files)
        return None

    def dump_workers_stack_traces(self) -> Optional[str]:
        """Fan out a native stack dump to every worker and write it to the log dir.

        Every rank gets a ``rank_<worker rank>.log`` in the uploaded folder. A rank
        whose dump could not be launched, timed out, or failed to collect gets a
        one-line placeholder saying so users know why it failed.

        Returns:
            The path to the folder with the stack traces.
        """
        workers = list(self._worker_group.get_workers())
        if not workers:
            return None

        dumps = fan_out_to_workers(
            workers,
            dump_stack_trace,
            _STACK_DUMP_TIMEOUT_S - 1,
            timeout_s=_STACK_DUMP_TIMEOUT_S,
        )
        files = {
            f"rank_{rank}.log": str(dump.value)
            if dump.error is None
            else str(dump.error)
            for rank, dump in dumps.items()
        }
        return self.upload_diagnostics(_STACK_TRACES_TOOL, files)

    def dump_workers_flight_recorder(self) -> Optional[str]:
        """Fan out a Flight Recorder dump to every worker and write it to the log dir.

        Every rank gets a ``rank_<world_rank>.json`` holding that rank's buffer:
        the collectives still in the ring with their process group, sequence id,
        state, input/output shapes and dtypes, and issuing call stack.

        Returns:
            The path to the folder with the dumps.
        """
        if TORCH_FR_BUFFER_SIZE_ENV_VAR not in os.environ:
            return None

        workers = list(self._worker_group.get_workers())
        if not workers:
            return None

        dumps = fan_out_to_workers(
            workers, dump_flight_recorder, timeout_s=_FLIGHT_RECORDER_DUMP_TIMEOUT_S
        )

        files: Dict[str, str] = {}
        for rank, diagnostic_result in dumps.items():
            error = diagnostic_result.error
            if error is not None:
                logger.info("No Flight Recorder dump from rank %d: %s", rank, error)
                files[f"rank_{rank}.json"] = json.dumps({"dump_error": str(error)})
                continue

            files[f"rank_{rank}.json"] = str(diagnostic_result.value)

        return self.upload_diagnostics(_FLIGHT_RECORDER_TOOL, files)

    def dump_nodes_nvidia_smi(self) -> Optional[str]:
        """Snapshot every node's GPUs and write the reports to the log dir.

        GPUs belong to the node rather than the rank, so exactly one worker
        per node is queried and each node gets a ``node_<node ip>.log``.

        Returns:
            The path to the folder with the snapshots.
        """
        node_workers: Dict[str, Worker] = {}
        node_ips: Dict[int, str] = {}
        for worker in self._worker_group.get_workers():
            node_workers.setdefault(worker.metadata.node_ip, worker)
            node_ips[worker.distributed_context.world_rank] = worker.metadata.node_ip
        if not node_workers:
            return None

        dumps = fan_out_to_workers(
            list(node_workers.values()),
            run_nvidia_smi,
            _NVIDIA_SMI_TIMEOUT_S - 1,
            timeout_s=_NVIDIA_SMI_TIMEOUT_S,
        )

        files: Dict[str, str] = {}
        for rank, dump in dumps.items():
            node_ip = node_ips[rank]
            files[f"node_{node_ip}.log"] = (
                str(dump.value)
                if dump.error is None
                else f"no `nvidia-smi` snapshot: {dump.error}"
            )

        return self.upload_diagnostics(_NVIDIA_SMI_TOOL, files)

    def upload_diagnostics(self, tool: str, files: Dict[str, str]) -> str:
        """Upload one tool's files to the run's storage filesystem.

        Args:
            tool: The sub-directory of ``hang_detector/`` to write to.
            files: ``{filename: contents}`` to write into that directory.

        Returns:
            The path the files were uploaded to, which survives cluster teardown.
        """
        storage_context = self._worker_group._storage_context
        fs_path = os.path.join(
            storage_context.experiment_fs_path, HANG_DETECTOR_DIRNAME, tool
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for name, contents in files.items():
                (Path(temp_dir) / name).write_text(contents)
            _upload_to_fs_path(temp_dir, storage_context.storage_filesystem, fs_path)
        return fs_path
