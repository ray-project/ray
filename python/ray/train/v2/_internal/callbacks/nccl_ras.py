"""NCCL RAS-based hang detector callback.

NCCL ships Reliability/Availability/Serviceability (RAS) subsystem (NCCL
>= 2.24) that runs a monitoring thread inside *every* NCCL process (one per
GPU/rank). Those threads form a peer mesh tracking job health detecting dead/
unresponsive ranks and per-rank collective op-counts. Polling ``ncclras``
we can check if a communicator's op counts between ranks are mismatched,
indicating a hang if the op counts don't increase over sequential polls.

Warning: RAS requires that all nodes are communicating, therefore, if there
is no "world" communicator with all ranks on, RAS will return a subset of
the ranks and communicators.
"""
import json
import logging
import math
import os
import re
import subprocess
import tempfile
import time
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Union

import ray
from ray._private.ray_constants import env_float
from ray.exceptions import GetTimeoutError
from ray.train.v2._internal.constants import (
    DEFAULT_NCCL_MIN_RAS_POLL_INTERVAL_S,
    DEFAULT_NCCL_RAS_ACTION,
    DEFAULT_NCCL_RAS_CONFIRM_DURATION_S,
    DEFAULT_NCCLRAS_BINARY_PATH,
    NCCL_RAS_ACTION_ENV_VAR,
    NCCL_RAS_ACTION_FAIL,
    NCCL_RAS_ACTION_OBSERVE,
    NCCL_RAS_ADDR_ENV_VAR,
    NCCL_RAS_CONFIRM_DURATION_S_ENV_VAR,
    NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR,
    NCCLRAS_BINARY_PATH_ENV_VAR,
)
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.storage import _upload_to_fs_path
from ray.train.v2._internal.execution.worker_group import WorkerGroup
from ray.train.v2.api.exceptions import NCCLHangError

logger = logging.getLogger(__name__)

# Query timeout lengths
_STACK_DUMP_TIMEOUT_S: float = 30.0
_NCCL_RAS_QUERY_TIMEOUT_S: float = 8.0  # the default ncclras -t value is 5

# User-facing escalation milestones
_FIRST_SUSPICION_AFTER_S: float = 60.0
_PERIODIC_WARN_EVERY_S: float = 120.0


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
        str(int(timeout_s)),
    ]
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


# ---------------------------------------------------------------------------
# TEMPORARY DEBUG -- remove before merge.
# `ncclras` is only the RAS *client*; the RAS *server* lives inside whichever
# libnccl.so the training process loaded (torch's bundled wheel, not the
# system/apt one). This probe reports both so a client/server version skew
# (e.g. a 2.28+ client sending `SET FORMAT json` to a 2.27 server) is visible.
# ---------------------------------------------------------------------------
def debug_nccl_provenance() -> str:
    """Report the `ncclras` client and the in-process NCCL library on a worker."""
    lines = [f"pid={os.getpid()} host={os.uname().nodename}"]

    for cmd in (
        ["whereis", "ncclras"],
        ["which", "-a", "ncclras"],
        ["ncclras", "--version"],
    ):
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            lines.append(
                f"$ {' '.join(cmd)} -> exit={proc.returncode} "
                f"stdout={proc.stdout.strip()!r} stderr={proc.stderr.strip()!r}"
            )
        except Exception as e:  # noqa: BLE001
            lines.append(f"$ {' '.join(cmd)} -> {type(e).__name__}: {e}")

    # The version that actually answers RAS queries.
    try:
        import torch

        lines.append(
            f"torch={torch.__version__} "
            f"torch.cuda.nccl.version()={torch.cuda.nccl.version()}"
        )
    except Exception as e:  # noqa: BLE001
        lines.append(f"in-process NCCL version unavailable: {type(e).__name__}: {e}")

    # Which libnccl.so.* the process has actually mapped.
    try:
        maps = Path("/proc/self/maps").read_text()
        mapped = sorted(set(re.findall(r"/\S*libnccl\S*", maps)))
        lines.append(f"mapped libnccl: {mapped}")
    except Exception as e:  # noqa: BLE001
        lines.append(f"/proc/self/maps unavailable: {type(e).__name__}: {e}")

    for var in ("LD_PRELOAD", "LD_LIBRARY_PATH", NCCL_RAS_ADDR_ENV_VAR):
        lines.append(f"{var}={os.environ.get(var)!r}")

    return "\n".join(lines)


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
    """

    timestamp: str
    comm_op_counts: Dict[str, Dict[int, Dict[str, int]]]
    comm_rank_status: Dict[str, Dict[int, str]]

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
        # TODO: `async_error` is ignored currently
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
        comm_op_counts, comm_rank_status = {}, {}
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

        return RASReport(data["timestamp"], comm_op_counts, comm_rank_status)
    except (KeyError, TypeError, ValueError) as e:
        logger.info(
            "NCCL RAS JSON did not match the expected schema: %s",
            e,
        )
        return None


class NCCLRASCallback(WorkerGroupCallback, ControllerCallback):
    """Detects NCCL hangs via the RAS subsystem (see module docstring for the
    topology and the hard/soft model).

    Default-on: registered by the trainer unless the hang detector
    (``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR``).

    To confirm that a NCCL anomaly isn't a single-snapshot blip, a communicator
    must stay frozen for consecutive polls, tracked as a per-communicator
    frozen-poll streak (so each communicator is confirmed on its own) and
    reset by any healthy poll. ``RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S``
    expresses how long that run should take and is converted to a poll count
    with the poll interval.
    """

    def __init__(self):
        self._binary_path = os.environ.get(
            NCCLRAS_BINARY_PATH_ENV_VAR, DEFAULT_NCCLRAS_BINARY_PATH
        )
        self._poll_interval_s = env_float(
            NCCL_RAS_MIN_POLL_INTERVAL_S_ENV_VAR, DEFAULT_NCCL_MIN_RAS_POLL_INTERVAL_S
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
        assert self._suspicion_polls >= 0
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

        # The train worker group to query for RAS
        self._worker_group: Optional[WorkerGroup] = None
        # Background executor for the RAS query to not block the controller event loop.
        self._executor: Optional[ThreadPoolExecutor] = None
        # In-flight background RAS query (see ``poll_ras_on_worker``), if any.
        self._ras_query_future: Optional[Future] = None
        # Force a query on the next poll after (re)start.
        self._last_query_time = float("-inf")

        # The previous successful poll's report
        self.prev_report: Optional[RASReport] = None
        # Per-communicator consecutive frozen-poll streaks ({comm_id: polls}).
        # As a deadlock requires the whole comm to be frozen (no op advancing),
        # any op progressing would indicate the comm overall isn't deadlocked.
        self.comm_deadlock_count: Dict[str, int] = {}

        # One-time degradation (e.g. missing binary) so we stop querying.
        self._is_ras_degraded: bool = False

    def reset_detection_state(self):
        """Full worker-group lifecycle reset (on (re)start / shutdown)."""
        self._ras_query_future = None
        self._last_query_time = float("-inf")
        self.prev_report = None

        self.reset_hang_counters()

    def reset_hang_counters(self):
        """Per-healthy-poll debounce reset `_prev_report` left for the next poll as comparison."""
        self.comm_deadlock_count = {}

    def after_worker_group_start(self, worker_group: WorkerGroup):
        self._worker_group = worker_group
        self.reset_detection_state()
        self.debug_log_nccl_provenance()  # TEMPORARY DEBUG -- remove before merge.

    def before_worker_group_shutdown(self, worker_group):
        self._worker_group = None
        # Abandon any in-flight query
        if self._ras_query_future is not None:
            self._ras_query_future.cancel()
            self._ras_query_future = None
        if self._executor is not None:
            self._executor.shutdown(wait=False)
            self._executor = None

    def after_worker_group_poll_status(self, worker_group_status):
        if self._is_ras_degraded or self._worker_group is None:
            return

        # This hook runs on the controller's poll loop, so any error here must
        # never crash training. A confirmed hang (NCCLHangError) is the intended
        # fail action and must propagate; every other exception is a detector bug
        # -- log it and disable detection for the rest of the run.
        try:
            ras_report = self.drive_ras_query()
            if ras_report is None:
                # No fresh query this poll due to throttling or an in-flight query
                return
            elif ras_report.mismatched_comms:
                self.evaluate_comm_mismatch(ras_report)
            else:  # Healthy with no mismatches
                if self.comm_deadlock_count:
                    for comm_id, count in self.comm_deadlock_count.items():
                        if count > self._suspicion_polls:
                            logger.info(
                                "NCCL communicator %s resumed making progress after "
                                "being stalled for %.0fs (%d polls). It is no longer "
                                "suspected of hanging.",
                                comm_id,
                                count * self._poll_interval_s,
                                count,
                            )

                self.reset_hang_counters()

            self.prev_report = ras_report
        except NCCLHangError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception(
                "NCCL RAS hang detection hit an unexpected error, therefore, "
                "disabling it for the rest of this training run."
            )
            self._is_ras_degraded = True

    def evaluate_comm_mismatch(self, report: RASReport):
        """Track frozen communicators and escalate user-facing hang messaging.

        A communicator is deadlocked only when *no* rank advanced *any* op since
        the last poll: a real hang blocks every rank, so every op freezes. Its
        possible for an op mismatch to occur and NCCL continue which isn't
        detected currently.
        """
        if self.prev_report is None:
            return

        op_diff = compute_report_op_diff(self.prev_report, report)

        new_comm_deadlock_count: Dict[str, int] = {}
        confirmed_comm_hangs: List[str] = []
        for comm_id in report.mismatched_comms:
            if comm_id not in op_diff:
                continue

            comm_frozen = all(
                delta == 0
                for op_deltas in op_diff[comm_id].values()
                for delta in op_deltas.values()
            )
            if not comm_frozen:
                continue

            count = self.comm_deadlock_count.get(comm_id, 0) + 1
            new_comm_deadlock_count[comm_id] = count

            if count == self._confirm_poll_counts:
                confirmed_comm_hangs.append(comm_id)

        # Handle confirmed comm hangs
        if confirmed_comm_hangs:
            ras_human_output = self.query_ras_on_workers("text")
            logger.warning("%s", ras_human_output)

            try:
                dump_dir = self.dump_workers_stack_traces()
            except Exception:
                logger.exception("Trying to dump worker stack traces failed.")
                dump_dir = None

            message = (
                f"{len(confirmed_comm_hangs)} of "
                f"{len(report.comm_op_counts)} communicators have a "
                f"collective mismatch and made no progress for "
                f"{self._confirm_duration_s:.0f} seconds "
                f"({self._confirm_poll_counts} polls). "
                "This usually means that the collective is deadlocked / hanging. "
                "The possible reasons for this is: a rank hit a divergent code "
                "path, exited early, a GPU or network hardware failure, or a "
                "collective was launched with a mismatched shape, dtype, or call order.\n"
                "To debug:\n"
                "  - Read NCCL RAS report in the logs (identifies the deadlocked ranks/communicators)\n"
                f"  - Your experiment directory contains the per-rank stack traces ({dump_dir})\n"
            )
            if self._action == NCCL_RAS_ACTION_FAIL:
                raise NCCLHangError(message, worker_failures={})
            elif self._action == NCCL_RAS_ACTION_OBSERVE:
                logger.warning(message)

        # Handle unfrozen comms
        for comm_id, count in self.comm_deadlock_count.items():
            if (
                comm_id not in new_comm_deadlock_count
                and self.comm_deadlock_count[comm_id] > self._suspicion_polls
            ):
                logger.info(
                    "NCCL communicator %s resumed making progress after being stalled "
                    "for %.0f seconds (%d polls). It is no longer suspected of hanging.",
                    comm_id,
                    count * self._poll_interval_s,
                    count,
                )

        # Any comm no longer frozen is dropped from the streak counts.
        self.comm_deadlock_count = new_comm_deadlock_count

        # Handle suspected frozen comms
        if self.comm_deadlock_count:
            total_comms = len(report.comm_op_counts)
            confirm_s = self._confirm_poll_counts * self._poll_interval_s
            escalation = (
                f"A NCCLHangError will be raised after {confirm_s:.0f} seconds if this persists."
                if self._action == NCCL_RAS_ACTION_FAIL
                else ""
            )

            # Announce communicators that just crossed the first-suspicion
            # threshold this poll so the user learns which are being watched.
            new_suspicions = [
                comm_id
                for comm_id, count in self.comm_deadlock_count.items()
                if count == self._suspicion_polls
            ]
            if new_suspicions:
                logger.warning(
                    "Possible NCCL hang detected! %d of %d communicators (%s) have "
                    "made no progress over %.0f seconds (%d consecutive polls). "
                    "Continuing to monitor, this might be a transient stall. %s",
                    len(new_suspicions),
                    total_comms,
                    ", ".join(new_suspicions),
                    self._suspicion_polls * self._poll_interval_s,
                    self._suspicion_polls,
                    escalation,
                )

            # Periodically remind about every still-frozen communicator in a
            # single message (with each one's stalled duration) and fetch the RAS
            # report once, rather than once per communicator.
            if any(
                count % self._periodic_warn_polls == 0
                for count in self.comm_deadlock_count.values()
            ):
                stalled = ", ".join(
                    f"{comm_id} for {count * self._poll_interval_s:.0f}s"
                    for comm_id, count in self.comm_deadlock_count.items()
                )
                periodic_escalation = ""
                if self._action == NCCL_RAS_ACTION_FAIL:
                    max_count = max(self.comm_deadlock_count.values())
                    remaining_polls = self._confirm_poll_counts - max_count
                    remaining_s = remaining_polls * self._poll_interval_s
                    periodic_escalation = (
                        f"A NCCLHangError will be raised in {remaining_s} seconds"
                        f"({remaining_polls} more polls) if this persists."
                    )
                logger.warning(
                    "NCCL hang still suspected! %d of %d communicators (%s) have made "
                    "no progress. %s",
                    len(self.comm_deadlock_count),
                    total_comms,
                    stalled,
                    periodic_escalation,
                )
                ras_human_output = self.query_ras_on_workers("text")
                if ras_human_output:
                    logger.info("%s", ras_human_output)

    def debug_log_nccl_provenance(self):
        """TEMPORARY DEBUG -- remove before merge.

        Log, once per worker-group start, where each worker's `ncclras` client
        comes from and which NCCL library that worker process actually loaded.
        """
        workers = list(self._worker_group.get_workers()) if self._worker_group else []
        refs = {}
        for worker in workers:
            try:
                refs[worker.execute_async(debug_nccl_provenance)] = worker
            except Exception as e:  # noqa: BLE001
                logger.warning("NCCL provenance probe failed to launch: %s", e)

        if not refs:
            return

        ready, not_ready = ray.wait(list(refs), num_returns=len(refs), timeout=20.0)
        for ref in not_ready:
            ray.cancel(ref)
        for ref in ready:
            try:
                logger.warning("NCCL provenance probe:\n%s", ray.get(ref))
            except Exception as e:  # noqa: BLE001
                logger.warning("NCCL provenance probe failed: %s", e)

    def drive_ras_query(self) -> Optional[RASReport]:
        """Drive the throttled JSON RAS poll without blocking the event loop.

        Only the periodic JSON poll goes through here. The one-off human-readable
        ``-f text`` report fetched at hang time uses :meth:`query_ras_text`
        directly so it doesn't share this method's single-in-flight future or
        poll-interval throttle.

        Returns:
            A report on the poll where a query becomes ready, else ``None``.
        """
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="nccl-ras-query"
            )

        now = time.monotonic()
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

    def query_ras_on_workers(
        self, ras_format: Literal["json", "text"]
    ) -> Optional[Union[RASReport, str]]:
        """Run RAS query across candidate workers (runs on the background thread).

        Tries each worker in turn and returns the first usable report. If every
        worker fails, logs the reason: fatal misconfigurations (missing/outdated
        ``ncclras`` binary) disable detection for the rest of the run, while
        transient failures (timeouts, exit codes) are logged for the poll and
        retried on the next one.

        Args:
            ras_format: What format to use with `ncclras`.

        Returns:
            The parsed report (``json``) / raw text (``text``), or ``None`` if no
            worker produced a usable result this poll.
        """
        workers = list(self._worker_group.get_workers())
        if not workers:
            logger.warning(
                "NCCL RAS: no workers available to query `ncclras`. "
                "Skipping this poll."
            )
            return None

        last_failure_reason: Optional[str] = None
        last_failure_stderr: Optional[str] = None
        for worker in workers:
            ref = None
            try:
                ref = worker.execute_async(
                    run_ncclras,
                    self._binary_path,
                    _NCCL_RAS_QUERY_TIMEOUT_S,
                    ras_format,
                )
                result = ray.get(ref, timeout=_NCCL_RAS_QUERY_TIMEOUT_S)
            except GetTimeoutError:
                last_failure_reason = "query_timeout"
                logger.debug(
                    "`ncclras` query timed out on worker %s. "
                    "Cancelling and trying the next worker.",
                    worker,
                )
                ray.cancel(ref)
                continue
            except Exception as e:  # noqa: BLE001
                last_failure_reason = f"query_error: {e}"
                logger.debug(
                    "`ncclras` query failed on worker %s: %s. "
                    "Trying the next worker.",
                    worker,
                    e,
                )
                if ray is not None:
                    ray.cancel(ref)
                continue

            if not result.get("ok"):
                last_failure_reason = result.get("reason")
                last_failure_stderr = result.get("stderr", None)
                logger.debug(
                    "`ncclras` on worker %s returned no data. "
                    "Reason: %s, stderr: %s. Trying the next worker.",
                    worker,
                    last_failure_reason,
                    last_failure_stderr,
                )
                continue

            if ras_format == "json":
                # Stash the raw JSON for the pre-fail snapshot / soft-hang logs.
                logger.debug("`ncclras` json output: %s", result["stdout"])
                report = parse_ras_schema(result["stdout"])
                if report is None:
                    last_failure_reason = "unparseable_json"
                    continue
                return report
            else:  # ras_format == 'text'
                if result["stdout"]:
                    return result["stdout"]
                last_failure_reason = "empty_text_output"
                continue

        # Every worker failed. Fatal, run-wide misconfigurations disable the
        # detector. Anything else is treated as transient and retried next poll.
        if last_failure_reason == "binary_not_found":
            logger.warning(
                "`ncclras` binary %r not found on any worker. "
                "Disabling NCCL RAS hang detection for the rest of this run. "
                "Set %s to a valid path.",
                self._binary_path,
                NCCLRAS_BINARY_PATH_ENV_VAR,
            )
            self._is_ras_degraded = True
        elif last_failure_reason == "unsupported_f_option":
            logger.warning(
                "`ncclras` binary %r rejected the `-f` format flag, "
                "which requires NCCL 2.28+. Disabling NCCL RAS hang detection "
                "for the rest of this run.",
                self._binary_path,
            )
            self._is_ras_degraded = True
        else:
            logger.info(
                "`ncclras` (%s) returned no usable data from any of "
                "%d worker(s) this poll (last reason: %s, last stderr: %s). "
                "Will retry next poll.",
                ras_format,
                len(workers),
                last_failure_reason,
                last_failure_stderr,
            )
        return None

    def dump_workers_stack_traces(self) -> Optional[str]:
        """Fan out a native stack dump to every worker and write it to the log dir.

        Returns:
            The path to the folder with the stack traces.
        """
        workers = list(self._worker_group.get_workers())
        if not workers:
            return None

        dump_refs = {}
        for rank, worker in enumerate(workers):
            try:
                ref = worker.execute_async(dump_stack_trace, _STACK_DUMP_TIMEOUT_S - 5)
                dump_refs[ref] = rank
            except Exception as e:  # noqa: BLE001
                logger.info("Failed to launch stack dump on worker %s: %s", worker, e)

        if not dump_refs:
            logger.info("Could not launch a stack dump on any worker.")
            return None

        ready, not_ready = ray.wait(
            list(dump_refs), num_returns=len(dump_refs), timeout=_STACK_DUMP_TIMEOUT_S
        )
        for ref in not_ready:
            ray.cancel(ref)

        with tempfile.TemporaryDirectory() as temp_dir:
            for ref in ready:
                try:
                    stack_trace = ray.get(ref)
                    with open(Path(temp_dir) / f"rank_{dump_refs[ref]}.log", "w") as f:
                        f.write(stack_trace)
                except Exception as e:  # noqa: BLE001
                    logger.info(
                        f"Failed to collect stack on rank {dump_refs[ref]}: {e}"
                    )

            stack_trace_folder = os.path.join(
                self._worker_group._storage_context.experiment_fs_path,
                "nccl_ras_hang_stack_traces",
            )
            _upload_to_fs_path(
                temp_dir,
                self._worker_group._storage_context.storage_filesystem,
                stack_trace_folder,
            )
            return stack_trace_folder
