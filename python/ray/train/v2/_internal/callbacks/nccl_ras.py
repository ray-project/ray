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
)
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.storage import _upload_to_fs_path
from ray.train.v2._internal.execution.worker_group import Worker, WorkerGroup
from ray.train.v2.api.exceptions import NCCLHangError

logger = logging.getLogger(__name__)

# Query timeout lengths
_STACK_DUMP_TIMEOUT_S: float = 30.0
_NVIDIA_SMI_TIMEOUT_S: float = 30.0
_NCCL_RAS_QUERY_TIMEOUT_S: float = 8.0  # the default ncclras -t value is 5

# Every diagnostic is uploaded to
# `<experiment_fs_path>/<HANG_DETECTOR_DIRNAME>/<tool>/`.
_STACK_TRACES_TOOL: str = "stack_traces"
_NCCL_RAS_TOOL: str = "nccl_ras"
_NVIDIA_SMI_TOOL: str = "nvidia_smi"

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


def _compact_ranks(ranks: List[int]) -> List[Tuple[str, int]]:
    """Collapse ranks into ``(text, size)`` runs, e.g. ``[("0-3", 4), ("5", 1)]``."""
    spans: List[List[int]] = []
    for rank in sorted(ranks):
        if spans and spans[-1][1] == rank - 1:
            spans[-1][1] = rank
        else:
            spans.append([rank, rank])
    return [(str(lo) if lo == hi else f"{lo}-{hi}", hi - lo + 1) for lo, hi in spans]


def format_ranks(train_ranks: List[int], unmatched_comm_ranks: List[int]) -> str:
    """Name a set of ranks compactly, e.g. ``train ranks 0-3, 5``.

    Consecutive ranks collapse into ranges, and the list is cut after
    ``_MAX_RANKS_LOGGED`` runs so a 1000-rank job stays one line.

    Args:
        train_ranks: Ray Train world ranks.
        unmatched_comm_ranks: Communicator ranks with no matching train
            worker, e.g. a process the user spawned.

    Returns:
        The train ranks, then the unmatched communicator ranks.
    """
    groups = [
        ("train rank", "", _compact_ranks(train_ranks), len(train_ranks)),
        (
            "comm rank",
            " (no matching train worker)",
            _compact_ranks(unmatched_comm_ranks),
            len(unmatched_comm_ranks),
        ),
    ]
    parts = []
    budget = _MAX_RANKS_LOGGED
    for noun, suffix, runs, total in groups:
        if not runs:
            continue
        shown = [text for text, _ in runs[:budget]]
        hidden = sum(size for _, size in runs[budget:])
        budget = max(1, budget - len(shown))
        if hidden:
            shown.append(f"and {hidden} more")
        plural = "" if total == 1 else "s"
        parts.append(f"{noun}{plural} {', '.join(shown)}{suffix}")
    return ", ".join(parts)


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

    Attributes:
        timestamp: str, when `ncclras` responded.
        comm_op_counts: Maps each communicator id to ``{global_rank: {op_name:
            count}}`` for ALL of that communicator's ranks (no filtering, no
            majority logic).
        comm_rank_status: Maps each communicator and their ranks with their
            status. A hang requires that the rank to be RUNNING.
        raw_json: The ``ncclras`` output this report was parsed from
        comm_rank_processes: Maps each communicator and their ranks to the
            ``(host, pid)`` of the process running that rank, used to find the
            Ray Train worker behind a communicator rank.
    """

    timestamp: str
    comm_op_counts: Dict[str, Dict[int, Dict[str, int]]]
    comm_rank_status: Dict[str, Dict[int, str]]
    raw_json: str = ""
    comm_rank_processes: Dict[str, Dict[int, Tuple[str, int]]] = field(
        default_factory=dict
    )

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
            comm_rank_processes[comm["hash"]] = {
                rank["rank"]: (rank.get("host", ""), int(rank.get("pid", -1)))
                for rank in comm["ranks"]
            }

        return RASReport(
            data["timestamp"],
            comm_op_counts,
            comm_rank_status,
            ras_json,
            comm_rank_processes,
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
        ref = None
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

        # Whether the `ncclras` text report was logged for the current
        # suspected hang, so it's logged once rather than on every warning.
        self._ras_text_logged: bool = False

        # One-time degradation (e.g. missing binary) so we stop querying.
        self._is_ras_degraded: bool = False

    def reset_detection_state(self):
        """Full worker-group lifecycle reset (on (re)start / shutdown)."""
        self.prev_report = None
        self.ras_history.clear()
        self.reset_hang_counters()

    def reset_hang_counters(self):
        """Per-healthy-poll debounce reset `_prev_report` left for the next poll as comparison."""
        self.comm_deadlock_count = {}
        self._ras_text_logged = False

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._worker_group = worker_group
        self.reset_detection_state()
        self._retire_poller()
        if self._is_ras_degraded:
            return
        self._ras_poller = RASPoller(worker_group, self._poll_interval_s)
        self._ras_poller.start()

    def before_worker_group_shutdown(self, worker_group):
        self._retire_poller()
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
                    "`ncclras` %s. Disabling NCCL RAS hang detection for the rest "
                    "of this run.",
                    result,
                )
                self._is_ras_degraded = True
                return

            self.ras_history.append(result)

            if result.mismatched_comms:
                self.evaluate_comm_mismatch(result)
            else:  # Healthy with no mismatches, so every frozen streak is over
                self.log_recovered_comms(frozen_counts={})
                self.reset_hang_counters()
            self.prev_report = result
        except NCCLHangError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception(
                "NCCL RAS hang detection hit an unexpected error, therefore, "
                "disabling it for the rest of this training run."
            )
            self._is_ras_degraded = True

    def evaluate_comm_mismatch(self, report: RASReport):
        """Advance the per-communicator frozen streaks and act on them.

        A communicator is deadlocked only when *no* rank advanced *any* op since
        the last poll: a real hang blocks every rank, so every op freezes. It's
        possible for an op mismatch to occur and NCCL continue which isn't
        detected currently.
        """
        if self.prev_report is None:
            return

        # 1. Classify: which mismatched communicators made no progress this poll
        frozen_counts = self.compute_frozen_streaks(report)
        confirmed_comm_hangs = [
            comm_id
            for comm_id, count in frozen_counts.items()
            if count == self._confirm_poll_counts
        ]

        # 2. Update state: a communicator that progressed drops its streak
        self.log_recovered_comms(frozen_counts)
        self.comm_deadlock_count = frozen_counts

        # 3. Act
        if confirmed_comm_hangs:
            self.handle_confirmed_hangs(confirmed_comm_hangs, report)
        elif frozen_counts:
            self.handle_suspected_hangs(report)

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

    def log_recovered_comms(self, frozen_counts: Dict[str, int]):
        """Log each previously suspected communicator that is no longer frozen.

        Args:
            frozen_counts: This poll's streaks; anything in
                ``comm_deadlock_count`` but not here has resumed progress.
        """
        for comm_id, count in self.comm_deadlock_count.items():
            if comm_id not in frozen_counts and count >= self._suspicion_polls:
                logger.info(
                    "NCCL communicator %s resumed making progress after being stalled "
                    "for %.0f seconds (%d polls). It is no longer suspected of hanging.",
                    comm_id,
                    count * self._poll_interval_s,
                    count,
                )

    def handle_confirmed_hangs(
        self, confirmed_comm_hangs: List[str], report: RASReport
    ):
        nvidia_smi_dir = self.capture_diagnostic(
            "nvidia-smi snapshots", self.dump_nodes_nvidia_smi
        )
        ras_history_dir = self.capture_diagnostic(
            "`ncclras` query history",
            lambda: self.dump_ras_query_history(self.fetch_ras_human_report()),
        )
        stack_trace_dir = self.capture_diagnostic(
            "worker stack traces", self.dump_workers_stack_traces
        )

        message = (
            f"NCCL hang detected: {len(confirmed_comm_hangs)} communicator(s) "
            "made no collective progress for "
            f"{self._confirm_duration_s:.0f} seconds "
            f"({self._confirm_poll_counts} polls) while their ranks disagree on "
            "how many collectives they have launched.\n"
            f"{self.describe_stalled_comms(report, confirmed_comm_hangs)}\n"
            "Common causes are a rank taking a divergent code path or exiting "
            "early, ranks launching collectives in a different order or with "
            "mismatched shapes or dtypes, or a GPU or network failure.\n"
        )
        if stack_trace_dir or ras_history_dir:
            message += "To debug:\n"
        if stack_trace_dir:
            message += (
                "  - Compare the stack traces of the ranks with different launch "
                f"counts: {stack_trace_dir}\n"
            )
        if ras_history_dir:
            message += (
                "  - NCCL's own report (ncclras_report.txt) and how the launch "
                f"counts drifted over the polls before the hang: {ras_history_dir}\n"
            )
        if nvidia_smi_dir:
            message += (
                "  - The per-node `nvidia-smi` snapshots show every GPU's power, "
                "temperature, clocks and ECC state at the moment of the hang, to "
                f"rule hardware out issues ({nvidia_smi_dir})\n"
            )
        if self._action == NCCL_RAS_ACTION_FAIL:
            raise NCCLHangError(message, worker_failures={})
        elif self._action == NCCL_RAS_ACTION_OBSERVE:
            logger.warning(message)

    def handle_suspected_hangs(self, report: RASReport):
        # A communicator first warns at the suspicion threshold, then every
        # `_periodic_warn_polls` after it.
        new_suspicions = [
            comm_id
            for comm_id, count in self.comm_deadlock_count.items()
            if count == self._suspicion_polls
        ]
        periodic_due = any(
            count > self._suspicion_polls
            and (count - self._suspicion_polls) % self._periodic_warn_polls == 0
            for count in self.comm_deadlock_count.values()
        )

        if new_suspicions:
            escalation = (
                "A NCCLHangError will be raised after "
                f"{self._confirm_duration_s:.0f} seconds if this persists."
                if self._action == NCCL_RAS_ACTION_FAIL
                else ""
            )
            logger.warning(
                "Possible NCCL hang detected! %d communicator(s) have made no "
                "collective progress for %.0f seconds while their ranks disagree "
                "on how many collectives they have launched. Continuing to "
                "monitor, this might be a transient stall. %s\n%s",
                len(new_suspicions),
                self._suspicion_polls * self._poll_interval_s,
                escalation,
                self.describe_stalled_comms(report, new_suspicions),
            )
        elif periodic_due:
            escalation = ""
            if self._action == NCCL_RAS_ACTION_FAIL:
                max_count = max(self.comm_deadlock_count.values())
                remaining_polls = self._confirm_poll_counts - max_count
                remaining_s = remaining_polls * self._poll_interval_s
                escalation = (
                    f"A NCCLHangError will be raised in {remaining_s:.0f} seconds "
                    f"({remaining_polls} more polls) if this persists."
                )
            logger.warning(
                "NCCL hang still suspected! %d communicator(s) have made no "
                "progress. %s\n%s",
                len(self.comm_deadlock_count),
                escalation,
                self.describe_stalled_comms(report, list(self.comm_deadlock_count)),
            )
        else:
            return

        # NCCL's own view of the hang, once per suspected hang; it is saved
        # with the diagnostics if the hang is confirmed.
        if not self._ras_text_logged:
            ras_human_output = self.fetch_ras_human_report()
            if ras_human_output:
                logger.info("%s", ras_human_output)
                self._ras_text_logged = True

    def index_train_workers(self) -> Dict[Tuple[str, int], Worker]:
        """Index the worker group by the process each worker runs in.

        RAS identifies a rank by the host and pid of its NCCL process, which for
        Ray Train is the worker actor's process, so ``(node_ip, pid)`` joins a
        RAS rank to its worker. A pid is only unique per node, hence the pair.

        Returns:
            ``{(node_ip, pid): worker}``, empty when there is no worker group.
        """
        if self._worker_group is None:
            return {}
        return {
            (worker.metadata.node_ip, worker.metadata.pid): worker
            for worker in self._worker_group.get_workers()
        }

    @staticmethod
    def find_train_worker(
        workers: Dict[Tuple[str, int], Worker], host: str, pid: int
    ) -> Optional[Worker]:
        """Find the Ray Train worker running a RAS rank's process.

        Falls back to the pid alone, when exactly one worker has it, because RAS
        can report the address of a different interface than Ray's node IP on
        multi-NIC nodes.

        Args:
            workers: The index from :meth:`index_train_workers`.
            host: The rank's host, as reported by RAS.
            pid: The rank's process id, as reported by RAS.

        Returns:
            The matching worker, or ``None`` if there isn't exactly one.
        """
        if (host, pid) in workers:
            return workers[(host, pid)]
        same_pid = [worker for (_, wpid), worker in workers.items() if wpid == pid]
        return same_pid[0] if len(same_pid) == 1 else None

    def train_ranks(self, report: RASReport, comm_id: str) -> Dict[int, Optional[int]]:
        """Translate a communicator's ranks into Ray Train world ranks.

        Args:
            report: The poll's report.
            comm_id: A communicator in ``report``.

        Returns:
            ``{comm_rank: train world rank}``, ``None`` for a rank with no
            matching train worker.
        """
        workers = self.index_train_workers()
        processes = report.comm_rank_processes.get(comm_id, {})
        translated: Dict[int, Optional[int]] = {}
        for rank in report.comm_op_counts[comm_id]:
            host, pid = processes.get(rank, ("", -1))
            worker = self.find_train_worker(workers, host, pid)
            translated[rank] = (
                worker.distributed_context.world_rank
                if worker is not None and worker.distributed_context is not None
                else None
            )
        return translated

    def describe_stalled_comms(self, report: RASReport, comm_ids: List[str]) -> str:
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
            comm_ids: The stalled communicators to describe.

        Returns:
            One line per communicator, or ``""`` if they couldn't be built.
        """
        try:
            lines = []
            for comm_id in comm_ids:
                translated = self.train_ranks(report, comm_id)

                def name(comm_ranks: List[int]) -> str:
                    return format_ranks(
                        [
                            translated[r]
                            for r in comm_ranks
                            if translated[r] is not None
                        ],
                        [r for r in comm_ranks if translated[r] is None],
                    )

                counts = report.comm_op_counts[comm_id]
                ops = []
                for op, skew in sorted(report.comm_op_skews[comm_id].items()):
                    if skew == 0:
                        continue
                    ranks_by_count: Dict[int, List[int]] = defaultdict(list)
                    for rank, op_counts in counts.items():
                        ranks_by_count[op_counts.get(op, 0)].append(rank)
                    ops.append(
                        f"{op} launches: "
                        + "; ".join(
                            f"{count} by {name(ranks_by_count[count])}"
                            for count in sorted(ranks_by_count, reverse=True)
                        )
                    )
                stalled_s = self.comm_deadlock_count.get(comm_id, 0) * (
                    self._poll_interval_s
                )
                lines.append(
                    f"  - Communicator {comm_id} over {name(list(translated))}, "
                    f"no progress for {stalled_s:.0f}s. {'. '.join(ops)}"
                )
            return "\n".join(lines)
        except Exception:  # noqa: BLE001
            logger.debug("Could not describe the stalled communicators.", exc_info=True)
            return ""

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
