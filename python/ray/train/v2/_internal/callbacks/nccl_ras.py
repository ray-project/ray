"""NCCL RAS-based hang detector callback.

NCCL ships a Reliability/Availability/Serviceability (RAS) subsystem (NCCL
>= 2.24) that runs a monitoring thread inside *every* NCCL process (one per
GPU/rank). Those threads form a peer mesh tracking job health, detecting dead/
unresponsive ranks and per-rank collective op-counts. By polling ``ncclras``
we can check if a communicator's collective counts between ranks are mismatched,
indicating a hang if the collective counts don't increase over sequential polls.

Warning: RAS only knows about the ranks it can reach through its mesh, and the
mesh is built from the communicators each process has created. Therefore, a
world process over all ranks is critical.
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
from ray.train.v2._internal.state.schema import (
    NcclCommunicator,
    NcclCommunicatorState,
    NcclHangDetectorStatus,
    NcclHangDisabledReason,
    TrainNcclHangEvent,
)
from ray.train.v2._internal.state.state_manager import NcclHangEventExporter
from ray.train.v2.api.exceptions import NCCLHangError

logger = logging.getLogger(__name__)

# User-facing escalation milestones
_FIRST_SUSPICION_AFTER_S: float = 60.0
_PERIODIC_WARN_EVERY_S: float = 120.0

# Every diagnostic is uploaded to
# `<experiment_fs_path>/<HANG_DETECTOR_DIRNAME>/<tool>/`.
_STACK_TRACES_TOOL: str = "stack_traces"
_NCCL_RAS_TOOL: str = "nccl_ras"
_NVIDIA_SMI_TOOL: str = "nvidia_smi"
_FLIGHT_RECORDER_TOOL: str = "flight_recorder"

# Query timeout lengths
_STACK_DUMP_TIMEOUT_S: float = 30.0
_NVIDIA_SMI_TIMEOUT_S: float = 30.0
_FLIGHT_RECORDER_DUMP_TIMEOUT_S: float = 30.0
_NCCL_RAS_QUERY_TIMEOUT_S: float = 8.0  # the default ncclras -t value is 5

# Polls of RAS history kept on top of confirmed ones, so the saved history
# can start before the first communicator stalled.
_RAS_HISTORY_POLL_MARGIN: int = 4

# Maximum number of Ranks named per list in a log line; the rest are
# summarised as a count so a 1000-rank job doesn't log 1000 ranks per poll.
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
        comm_op_counts: Maps each communicator id to ``{global_rank: {op_name:
            count}}`` for ALL of that communicator's ranks (no filtering, no
            majority logic).
        comm_rank_status: Maps each communicator and their ranks with their
            status. A hang requires that the rank to be RUNNING.
        comm_rank_processes: Maps each communicator and their ranks to the
            ``(host, pid)`` of the process running that rank, used to find the
            Ray Train worker behind a communicator rank.
        taken_at_ms: When the query returned, on the controller's clock, in
            ms since the UNIX epoch. ``ncclras``'s own timestamp isn't used as
            it can come from the clock of whichever worker's node answered,
            which can differ between polls.
        raw_json: The ``ncclras`` output this report was parsed from
    """

    comm_op_counts: Dict[str, Dict[int, Dict[str, int]]]
    comm_rank_status: Dict[str, Dict[int, str]]
    comm_rank_processes: Dict[str, Dict[int, Tuple[str, int]]]

    taken_at_ms: int
    raw_json: str

    @property
    def comm_op_skews(self) -> Dict[str, Dict[str, int]]:
        """Per-communicator, per-op spread (max-min) across its ranks, this poll.

        Returns:
            ``{comm_id: {op_name: max_count - min_count for ranks}}``.
        """
        skews: Dict[str, Dict[str, int]] = {}
        for comm_id, ranks in self.comm_op_counts.items():
            rank_op_counts = defaultdict(list)
            for op_counts in ranks.values():
                for op, count in op_counts.items():
                    rank_op_counts[op].append(count)
            skews[comm_id] = {
                op: max(counts) - min(counts) for op, counts in rank_op_counts.items()
            }
        return skews

    @property
    def mismatched_comms(self) -> Set[str]:
        """Communicator ids whose ranks have op counts that are mismatched and its ranks are RUNNING."""
        return {
            comm_id
            for comm_id, op_skews in self.comm_op_skews.items()
            if any(skews > 0 for skews in op_skews.values())
            and all(
                rank_status == "RUNNING"
                for rank_status in self.comm_rank_status[comm_id].values()
            )
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
        for op, skew in self.comm_op_skews[comm_id].items():
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


def parse_ras_schema(ras_json: str, taken_at_ms: int) -> Optional[RASReport]:
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
        taken_at_ms: When the query returned, in ms since the UNIX epoch.

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
            comm_rank_processes[comm["hash"]] = {
                rank["rank"]: (rank["host"], int(rank["pid"])) for rank in comm["ranks"]
            }

        return RASReport(
            comm_op_counts=comm_op_counts,
            comm_rank_status=comm_rank_status,
            comm_rank_processes=comm_rank_processes,
            taken_at_ms=taken_at_ms,
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
    thread. :meth:`query_text` is also used synchronously by the callback for
    the human-readable ``-f text`` report at hang time.
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

    def query_text(self) -> Optional[str]:
        """Synchronously fetch ``ncclras -f text`` for the logs.

        Runs on the caller's thread; only called once a hang is suspected or
        confirmed, when the job is already stalled.

        Returns:
            The report, or ``None`` if no worker could produce one.
        """
        try:
            return self.query("text")
        except RASQueryError as e:
            logger.info("Could not fetch the `ncclras` text report (%s).", e)
            return None

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
            taken_at_ms = int(time.time() * 1000)
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
            report = parse_ras_schema(result["stdout"], taken_at_ms)
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


def dump_nvidia_smi(timeout_s: float) -> DiagnosticResult:
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


def collect_stack_traces(workers: List[Worker]) -> Dict[str, str]:
    """Fan out a native stack dump to every worker.

    Args:
        workers: The train workers to dump.

    Returns:
        ``{"rank_<world rank>.log": stack trace}``. A rank whose dump could not
        be launched, timed out, or failed to collect holds why instead.
    """
    dumps = fan_out_to_workers(
        workers,
        dump_stack_trace,
        _STACK_DUMP_TIMEOUT_S - 1,
        timeout_s=_STACK_DUMP_TIMEOUT_S,
    )
    return {
        f"rank_{rank}.log": str(dump.value) if dump.error is None else str(dump.error)
        for rank, dump in dumps.items()
    }


def collect_flight_recorder(workers: List[Worker]) -> Dict[str, str]:
    """Fan out a Flight Recorder dump to every worker.

    Each rank's buffer holds the collectives still in the ring with their
    process group, sequence id, state, input/output shapes and dtypes, and
    issuing call stack.

    Args:
        workers: The train workers to dump.

    Returns:
        ``{"rank_<world rank>.json": dump}``, or ``{}`` when the ring buffer
        wasn't armed. A rank without a dump holds ``{"dump_error": ...}``.
    """
    if TORCH_FR_BUFFER_SIZE_ENV_VAR not in os.environ:
        return {}

    dumps = fan_out_to_workers(
        workers, dump_flight_recorder, timeout_s=_FLIGHT_RECORDER_DUMP_TIMEOUT_S
    )

    files: Dict[str, str] = {}
    for rank, dump in dumps.items():
        if dump.error is not None:
            logger.info("No Flight Recorder dump from rank %d: %s", rank, dump.error)
            files[f"rank_{rank}.json"] = json.dumps({"dump_error": str(dump.error)})
        else:
            files[f"rank_{rank}.json"] = str(dump.value)
    return files


def collect_nvidia_smi(workers: List[Worker]) -> Dict[str, str]:
    """Snapshot every node's GPUs.

    GPUs belong to the node rather than the rank, so exactly one worker per
    node is queried.

    Args:
        workers: The train workers, possibly several per node.

    Returns:
        ``{"node_<node ip>.log": nvidia-smi report}``.
    """
    node_workers: Dict[str, Worker] = {}
    node_ips: Dict[int, str] = {}
    for worker in workers:
        node_workers.setdefault(worker.metadata.node_ip, worker)
        node_ips[worker.distributed_context.world_rank] = worker.metadata.node_ip

    dumps = fan_out_to_workers(
        list(node_workers.values()),
        dump_nvidia_smi,
        _NVIDIA_SMI_TIMEOUT_S - 1,
        timeout_s=_NVIDIA_SMI_TIMEOUT_S,
    )
    return {
        f"node_{node_ips[rank]}.log": (
            str(dump.value)
            if dump.error is None
            else f"no `nvidia-smi` snapshot: {dump.error}"
        )
        for rank, dump in dumps.items()
    }


def collect_ras_history(
    history: Iterable[RASReport], text_report: Optional[str]
) -> Dict[str, str]:
    """The retained RAS polls, plus NCCL's text report.

    Args:
        history: The retained polls.
        text_report: The ``ncclras -f text`` report fetched at confirmation,
            or ``None`` if no worker could produce one.

    Returns:
        Each poll's JSON, named by when it was taken in UTC to the
        millisecond (e.g. ``ncclras_20260619T065156.123Z.json``), and
        ``ncclras_report.txt``.
    """
    files = {}
    for report in history:
        seconds, ms = divmod(report.taken_at_ms, 1000)
        utc = datetime.fromtimestamp(seconds, tz=timezone.utc)
        files[f"ncclras_{utc:%Y%m%dT%H%M%S}.{ms:03d}Z.json"] = report.raw_json
    if text_report:
        files["ncclras_report.txt"] = text_report
    return files


def translate_ras_ranks_to_train(
    workers: List[Worker], comm_rank_processes: Dict[int, Tuple[str, int]]
) -> Dict[int, int]:
    """Translate a communicator's ranks into Ray Train world ranks.

    A pid is only unique per node, so ``(node_ip, pid)`` is matched first;
    the pid alone is the fallback when exactly one worker has it.

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

    Consecutive ranks collapse into ranges, and each list is cut after
    ``_MAX_RANKS_LOGGED`` runs so a 1000-rank job stays one line.

    Args:
        comm_ranks: Communicator ranks to name.
        train_rank_of: The translation from :func:`match_train_ranks`.

    Returns:
        The train ranks, then the unmatched communicator ranks.
    """
    comm_ranks = list(comm_ranks)
    groups = [
        ("train rank", [train_rank_of[r] for r in comm_ranks if r in train_rank_of]),
        ("comm rank", [r for r in comm_ranks if r not in train_rank_of]),
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


@dataclass
class CommStall:
    """A communicator's stall metadata."""

    # Consecutive frozen polls.
    polls: int
    # Timestamp when the stall began
    since_ms: int
    # Whether the stall reached the confirmation duration
    confirmed: bool = False


@dataclass
class PollOutcome:
    """The findings between the latest and previous ras report."""

    # Communicators the suspicion warning names, and whether they newly
    # reached the threshold or the periodic reminder.
    suspected_comms: List[str] = field(default_factory=list)
    new_suspicion: bool = False
    periodic_reminder: bool = False
    # Communicators confirmed on this poll.
    confirmed_comms: List[str] = field(default_factory=list)
    # Suspected stalls that ended on this poll, as they were when they ended.
    recovered_comms: Dict[str, CommStall] = field(default_factory=dict)


class NCCLRASCallback(WorkerGroupCallback, ControllerCallback):
    """Detects NCCL hangs via the RAS subsystem.

    Default-off: the trainer only registers this callback when
    ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1`` is set on the driver.

    To confirm that a NCCL anomaly isn't a single-snapshot blip, a communicator
    must stay frozen for consecutive polls, tracked as a per-communicator
    frozen-poll streak (so each communicator is confirmed on its own) and
    reset by any healthy poll. ``RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S``
    expresses how long that run should take and is converted to a poll count
    with the poll interval. Once a communicator's hang has been confirmed,
    then a suite of diagnostic tools are saved: flight-recorder, nvidia-smi,
    ncclras history, and stack trace. These tools are saved to the Ray Train's
    storage path.
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
        # Circular buffer of ncclras json queries for diagnostics
        self.ras_history: Deque[RASReport] = deque(
            maxlen=self._confirm_poll_counts + _RAS_HISTORY_POLL_MARGIN
        )
        # Per-communicator open stalls ({comm_id: stall}).
        # As a deadlock requires the whole comm to be frozen (no op advancing),
        # any op progressing would indicate the comm overall isn't deadlocked.
        self.comm_stalls: Dict[str, CommStall] = {}

        # One-time degradation (e.g. missing binary) so we stop querying.
        self._is_ras_degraded: bool = False

        # Identify the exported events, set when a worker group starts.
        self._run_id: str = ""
        self._attempt_id: str = ""
        self._poll_index: int = 0

        self._event_exporter = NcclHangEventExporter()

    def after_worker_group_start(self, worker_group: WorkerGroup):
        # A new worker group is a new set of communicators, so detection
        # starts afresh.
        self._worker_group = worker_group
        self.prev_report = None
        self.ras_history.clear()
        self.comm_stalls = {}
        self._poll_index = 0
        self._run_id = worker_group._train_run_context.run_id
        self._attempt_id = worker_group.get_worker_group_context().run_attempt_id
        if self._is_ras_degraded:
            return
        self._ras_poller = RASPoller(worker_group, self._poll_interval_s)
        self._ras_poller.start()

    def before_worker_group_shutdown(self, worker_group: WorkerGroup):
        if self._ras_poller is not None:
            self._ras_poller.stop()
            self._ras_poller = None
        # An attempt that ends with a hung communicator ends with STOPPED,
        # unless the detector was disabled, which already ended its events.
        if not self._is_ras_degraded and any(
            stall.polls >= self._suspicion_polls
            for comm_id, stall in self.comm_stalls.items()
        ):
            self._poll_index += 1
            self.export_event(NcclHangDetectorStatus.STOPPED, int(time.time() * 1000))
        self._worker_group = None

    def before_worker_group_abort(self, worker_group_context):
        # An abort skips `before_worker_group_shutdown`, but ends the attempt
        self.before_worker_group_shutdown(self._worker_group)

    def disable(self, reason: NcclHangDisabledReason):
        """Disable the detector for the rest of the run, exporting DISABLED.

        Args:
            reason: Why the detector is disabled.
        """
        self._is_ras_degraded = True
        if self._ras_poller is not None:
            self._ras_poller.stop()
            self._ras_poller = None
        self._poll_index += 1
        self.export_event(
            NcclHangDetectorStatus.DISABLED,
            int(time.time() * 1000),
            disabled_reason=reason,
        )

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
            elif isinstance(result, RASQueryError):
                logger.warning(
                    "Disabling Ray Train NCCL hang detector for the rest of this run as `ncclras` returns %s.",
                    result,
                )
                self.disable(NcclHangDisabledReason.RAS_UNAVAILABLE)
                return

            # Result is a RASReport
            self._poll_index += 1
            self.ras_history.append(result)

            # Determine the poll outcome and what to do
            outcome: PollOutcome = self.evaluate_ras_poll(result)
            self.prev_report = result

            if outcome.confirmed_comms:
                diagnostics, ras_text_report = self.capture_diagnostics()
                # Exported before the action, so the event exists even if it raises.
                if any(diagnostics.values()):
                    diagnostic_dir = os.path.join(
                        self._worker_group._storage_context.experiment_fs_path,
                        HANG_DETECTOR_DIRNAME,
                    )
                else:
                    diagnostic_dir = None

                self.export_event(
                    NcclHangDetectorStatus.MONITORING,
                    result.taken_at_ms,
                    ras_text_report=ras_text_report,
                    diagnostics_dir=diagnostic_dir,
                )
                self.build_hang_report(result, outcome, diagnostics)

            elif (
                outcome.new_suspicion
                or outcome.periodic_reminder
                or outcome.recovered_comms
            ):
                # NCCL's own view of the hang, fetched once per new suspicion
                if outcome.new_suspicion:
                    ras_text_report = self._ras_poller.query_text()
                else:
                    ras_text_report = None

                self.log_suspected_and_recovered_comms(result, outcome, ras_text_report)
                self.export_event(
                    NcclHangDetectorStatus.MONITORING,
                    result.taken_at_ms,
                    ras_text_report=ras_text_report,
                )

        except NCCLHangError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception(
                "The Ray Train NCCL hang detector hit an unexpected error, therefore, "
                "disabling it for the rest of this training run."
            )
            self.disable(NcclHangDisabledReason.INTERNAL_ERROR)

    def evaluate_ras_poll(self, report: RASReport) -> PollOutcome:
        """Using the latest ras poll, update the per-communicator stalls.

        Args:
            report: The current poll's report. A stall that begins on this
                poll is dated from the previous report, whose counts were
                already frozen.

        Returns:
            What the poll found. It only decides; nothing is logged or acted on.
        """
        # Extend the stall of every mismatched communicator that made no
        # progress this poll (the first poll has nothing to compare against).
        # Communicators that progressed, or are new this poll, have no stall.
        updated_stalls: Dict[str, CommStall] = {}
        if self.prev_report is not None:
            op_diff = compute_report_op_diff(self.prev_report, report)

            for comm_id in report.mismatched_comms:
                if comm_id not in op_diff:
                    continue

                comm_frozen = all(
                    delta == 0
                    for op_deltas in op_diff[comm_id].values()
                    for delta in op_deltas.values()
                )
                if comm_frozen:
                    stall = self.comm_stalls.get(comm_id) or CommStall(
                        polls=0, since_ms=self.prev_report.taken_at_ms
                    )
                    stall.polls += 1
                    updated_stalls[comm_id] = stall

        # Decide which milestones the latest poll reached.
        max_stall_polls = max(
            (stall.polls for stall in updated_stalls.values()), default=0
        )
        outcome = PollOutcome(
            suspected_comms=[
                comm_id
                for comm_id, stall in updated_stalls.items()
                if stall.polls >= self._suspicion_polls
            ],
            new_suspicion=any(
                stall.polls == self._suspicion_polls
                for comm_id, stall in updated_stalls.items()
            ),
            periodic_reminder=(
                max_stall_polls > self._suspicion_polls
                and (max_stall_polls - self._suspicion_polls)
                % self._periodic_warn_polls
                == 0
            ),
            confirmed_comms=[
                comm_id
                for comm_id, stall in updated_stalls.items()
                if stall.polls == self._confirm_poll_counts
            ],
            # A suspected communicator that collective count increased will end its stall
            recovered_comms={
                comm_id: stall
                for comm_id, stall in self.comm_stalls.items()
                if comm_id not in updated_stalls
                and stall.polls >= self._suspicion_polls
            },
        )
        self.comm_stalls = updated_stalls
        for comm_id in outcome.confirmed_comms:
            self.comm_stalls[comm_id].confirmed = True

        return outcome

    def log_suspected_and_recovered_comms(
        self,
        report: RASReport,
        outcome: PollOutcome,
        ras_text_report: Optional[str],
    ) -> None:
        """Log the poll's recoveries and suspicion warning.

        Args:
            report: The current poll's report.
            outcome: The poll's outcome.
            ras_text_report: The ``ncclras -f text`` report fetched for a new
                suspicion, or ``None``.
        """
        if outcome.recovered_comms:
            logger.info(
                "The Ray Train NCCL hang detector callback detects that %d NCCL "
                "communicator(s) resumed making progress and are no longer "
                "suspected of hanging: %s",
                len(outcome.recovered_comms),
                ", ".join(
                    f"{comm_id} (stalled for "
                    f"{stall.polls * self._poll_interval_s:.0f}s, {stall.polls} polls)"
                    for comm_id, stall in sorted(outcome.recovered_comms.items())
                ),
            )

        if outcome.new_suspicion or outcome.periodic_reminder:
            if outcome.new_suspicion:
                headline = (
                    "Possible NCCL hang detected by the Ray Train NCCL hang detector callback! "
                    f"{len(outcome.suspected_comms)} communicator(s) have made no collective "
                    "progress while their ranks disagree on how many collectives they have "
                    "launched. Continuing to monitor, this might be a transient stall."
                )
            else:
                headline = (
                    "NCCL hang still suspected by the Ray Train NCCL hang detector callback! "
                    f"{len(outcome.suspected_comms)} communicator(s) have made no "
                    "collective progress."
                )

            if self._action == NCCL_RAS_ACTION_FAIL:
                remaining_polls = self._confirm_poll_counts - max(
                    stall.polls for stall in self.comm_stalls.values()
                )
                headline += (
                    " A NCCLHangError will be raised in "
                    f"{remaining_polls * self._poll_interval_s:.0f} seconds "
                    f"({remaining_polls} more polls) if this persists."
                )
            logger.warning(
                "%s\n%s",
                headline,
                self.describe_stalled_comms(report, outcome.suspected_comms),
            )

            # NCCL's own view of the hang, fetched once per suspected hang
            if ras_text_report:
                logger.info(
                    "NCCL's own `ncclras` report of the suspected hang (logged "
                    "once per hang). Its ranks are numbered per communicator, "
                    "not by Ray Train world rank:\n%s",
                    ras_text_report.rstrip(),
                )

    def describe_stalled_comms(
        self, report: RASReport, stalled_comm_ids: List[str]
    ) -> str:
        """One line per communicator with its launch counts by train rank.

        NCCL numbers a communicator's ranks from 0, which differs from the
        Ray Train world rank on any communicator but the world one, so every
        rank is translated, e.g. ``AllReduce launches: 622 by train rank 2;
        621 by train rank 3``. Only the collectives whose counts disagree are
        listed.

        Args:
            report: The current poll's report.
            stalled_comm_ids: The stalled communicators to describe.

        Returns:
            One line per communicator, or ``""`` if they couldn't be built.
        """

        try:
            workers = self._worker_group.get_workers() if self._worker_group else []
            described_stalls = []
            for comm_id in stalled_comm_ids:
                train_rank_of = translate_ras_ranks_to_train(
                    workers, report.comm_rank_processes.get(comm_id, {})
                )
                ops = [
                    f"{op} launches: "
                    + "; ".join(
                        f"{count} by {format_ranks(ranks, train_rank_of)}"
                        for count, ranks in sorted(ranks_by_count.items(), reverse=True)
                    )
                    for op, ranks_by_count in sorted(
                        report.mismatched_op_ranks(comm_id).items()
                    )
                ]
                comm_ranks = format_ranks(report.comm_op_counts[comm_id], train_rank_of)
                stalled_s = self.comm_stalls[comm_id].polls * self._poll_interval_s
                described_stalls.append(
                    f"  - Communicator {comm_id} over {comm_ranks}, "
                    f"no progress for {stalled_s:.0f}s. {'. '.join(ops)}"
                )
            return "\n".join(described_stalls)

        except Exception:  # noqa: BLE001
            logger.debug("Could not describe the stalled communicators.", exc_info=True)
            return ""

    def build_hang_report(
        self,
        report: RASReport,
        outcome: PollOutcome,
        diagnostics: Dict[str, Optional[str]],
    ):
        """Raise (fail mode) or log (observe mode) the confirmed hangs.

        Args:
            report: The current poll's report.
            outcome: The poll's outcome.
            diagnostics: Where each diagnostic was uploaded, ``{tool: dir}``,
                ``None`` for the ones that captured nothing.
        """
        message = (
            "NCCL hang detected by the Ray Train hang detector callback! "
            f"{len(outcome.confirmed_comms)} communicator(s) made no collective "
            f"progress for {self._confirm_duration_s:.0f} seconds "
            f"({self._confirm_poll_counts} polls) while their ranks disagree on "
            "how many collectives they have launched.\n"
            f"{self.describe_stalled_comms(report, outcome.confirmed_comms)}\n"
            "Common causes are a rank taking a divergent code path or exiting "
            "early, ranks launching collectives in a different order or with "
            "mismatched shapes or dtypes, or a GPU or network failure.\n"
        )
        stack_trace_dir = diagnostics.get(_STACK_TRACES_TOOL)
        ras_history_dir = diagnostics.get(_NCCL_RAS_TOOL)
        nvidia_smi_dir = diagnostics.get(_NVIDIA_SMI_TOOL)
        flight_recorder_dir = diagnostics.get(_FLIGHT_RECORDER_TOOL)
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
                "Find the collective in flight on each rank, with its op type, "
                "shapes and call stack, in the per-rank PyTorch Flight Recorder "
                f"dumps: {flight_recorder_dir}"
            )
        if debug_steps:
            message += "To debug:\n" + "".join(f"  - {step}\n" for step in debug_steps)

        if self._action == NCCL_RAS_ACTION_FAIL:
            raise NCCLHangError(message, worker_failures={})
        elif self._action == NCCL_RAS_ACTION_OBSERVE:
            logger.warning(message)

    def capture_diagnostics(self) -> Tuple[Dict[str, Optional[str]], Optional[str]]:
        """Capture and upload every diagnostic of a confirmed hang.

        Each tool's files are uploaded to
        ``<experiment_fs_path>/<HANG_DETECTOR_DIRNAME>/<tool>/``. A diagnostic
        that fails is logged rather than raised, so it never takes the hang
        handling with it, and one with nothing to record uploads nothing.

        Returns:
            ``{tool: uploaded dir or None}`` and the ``ncclras -f text``
            report fetched for the history, or ``None`` if it couldn't be.
        """
        workers = list(self._worker_group.get_workers())
        storage_context = self._worker_group._storage_context
        diagnostics: Dict[str, Optional[str]] = {}
        ras_text_report: Optional[str] = None

        def ras_history() -> Dict[str, str]:
            nonlocal ras_text_report
            ras_text_report = self._ras_poller.query_text()
            return collect_ras_history(self.ras_history, ras_text_report)

        # Flight Recorder first, before ranks still running overwrite its ring
        # buffer; nvidia-smi before the slow stack traces, so the GPU reading
        # is as close to the moment of the hang as possible.
        collectors: Dict[str, Callable[[], Dict[str, str]]] = {
            _FLIGHT_RECORDER_TOOL: lambda: collect_flight_recorder(workers),
            _NVIDIA_SMI_TOOL: lambda: collect_nvidia_smi(workers),
            _NCCL_RAS_TOOL: ras_history,
            _STACK_TRACES_TOOL: lambda: collect_stack_traces(workers),
        }
        for tool, collect in collectors.items():
            diagnostics[tool] = None
            try:
                files = collect()
                if not files:
                    continue
                fs_path = os.path.join(
                    storage_context.experiment_fs_path, HANG_DETECTOR_DIRNAME, tool
                )
                with tempfile.TemporaryDirectory() as temp_dir:
                    for name, contents in files.items():
                        (Path(temp_dir) / name).write_text(contents)
                    _upload_to_fs_path(
                        temp_dir, storage_context.storage_filesystem, fs_path
                    )
                diagnostics[tool] = fs_path
            except Exception:  # noqa: BLE001
                logger.exception("Trying to capture the %s diagnostics failed.", tool)
        return diagnostics, ras_text_report

    def export_event(
        self,
        status: NcclHangDetectorStatus,
        timestamp_ms: int,
        disabled_reason: Optional[NcclHangDisabledReason] = None,
        ras_text_report: Optional[str] = None,
        diagnostics_dir: Optional[str] = None,
    ):
        """Export an event listing the communicators hung right now.

        Hands it to the Train state actor, fire-and-forget; the exporter never
        raises, so exporting can never affect detection.

        Args:
            status: MONITORING for a poll that reached a milestone, STOPPED
                or DISABLED when the detector stops watching the attempt.
            timestamp_ms: When the event happened, in ms since the UNIX epoch.
            disabled_reason: Why the detector was disabled, for DISABLED.
            ras_text_report: The ``ncclras -f text`` report fetched on this
                poll, for MONITORING.
            diagnostics_dir: Where the diagnostics were uploaded, on the poll
                that confirmed a communicator.
        """
        communicators = [
            NcclCommunicator(
                communicator_id=comm_id,
                state=(
                    NcclCommunicatorState.CONFIRMED
                    if stall.confirmed
                    else NcclCommunicatorState.SUSPECTED
                ),
                stalled_since_ms=stall.since_ms,
            )
            for comm_id, stall in sorted(self.comm_stalls.items())
            if stall.polls >= self._suspicion_polls
        ]
        self._event_exporter.export(
            TrainNcclHangEvent(
                run_id=self._run_id,
                attempt_id=self._attempt_id,
                poll_index=self._poll_index,
                timestamp_ms=timestamp_ms,
                detector_status=status,
                disabled_reason=disabled_reason,
                ras_text_report=ras_text_report or None,
                diagnostics_dir=diagnostics_dir,
                communicators=communicators,
            )
        )
