"""Evaluators of the NCCL hang detector on ``ray.train.health``.

``nccl_hang_check()`` is the in-flight check that Ray Train adds to a run with
``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1``. It detects hangs like
``NCCLRASCallback``, which stays until it's deleted:

- ``NCCLRASProbe`` queries NCCL RAS with ``ncclras -f json`` on each worker
  node, every ``poll_interval_s``.
- ``NCCLHangEvaluator`` counts, for each communicator whose ranks launched
  different numbers of an op, the consecutive reports in which no rank advanced
  any op. Once a communicator is suspected, it warns and diagnoses with NCCL's
  text report, which ``LogRASTextReport`` logs. Once the hang is confirmed, it
  diagnoses with the Flight Recorder, ``nvidia-smi``, the RAS text report and
  the stack traces.
- ``HangDiagnostics`` uploads those diagnostics, then reattempts (``fail``) or
  only logs the hang (``observe``).

The evaluators reach the run's workers, storage and metrics through an
``NCCLHangRun``, which Ray Train supplies.

Warning: RAS only knows about the ranks it can reach through its mesh, and the
mesh is built from the communicators each process has created. A query
therefore returns every rank reachable from the queried node, which is the
whole job only if some communicator spans all ranks, such as the default
process group ``torch.distributed`` creates.
"""
import abc
import json
import logging
import math
import os
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)

from ray.train.health.check import HealthCheck, PeriodicHealthCheck
from ray.train.health.decision import Diagnose, HealthDecision, Noop, Reattempt
from ray.train.health.evaluator import Evaluator
from ray.train.health.probe import Probe, ProbeResult
from ray.train.health.probes.nccl_ras import (
    FlightRecorderProbe,
    NCCLRASProbe,
    NCCLRASTextProbe,
    NvidiaSmiProbe,
    StackTraceProbe,
)
from ray.train.health.state import HealthState

logger = logging.getLogger(__name__)

# What a confirmed hang does.
NCCL_HANG_ACTION_FAIL = "fail"  # reattempts the run
NCCL_HANG_ACTION_OBSERVE = "observe"  # logs the hang and its diagnostics

# PyTorch's variable that arms the Flight Recorder's buffer.
TORCH_FR_BUFFER_SIZE_ENV_VAR = "TORCH_FR_BUFFER_SIZE"

# The diagnostics, by the folder each one is uploaded to.
FLIGHT_RECORDER_TOOL = "flight_recorder"
NVIDIA_SMI_TOOL = "nvidia_smi"
NCCL_RAS_TOOL = "nccl_ras"
STACK_TRACES_TOOL = "stack_traces"

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


@dataclass
class NCCLHangReattempt(Reattempt):
    """The ``Reattempt`` of a confirmed NCCL hang, which Ray Train raises as
    ``NCCLHangError``."""


class NCCLHangDetectorState(Enum):
    """What the NCCL hang detector believes about the run."""

    HEALTHY = "HEALTHY"
    SUSPECTED = "SUSPECTED"
    CONFIRMED = "CONFIRMED"
    DEGRADED = "DEGRADED"


class WorkerInfo(NamedTuple):
    """A training worker, as the NCCL hang detector names it.

    Attributes:
        world_rank: The worker's Ray Train world rank.
        node_id: The ID of the worker's node.
        node_ip: The IP address of the worker's node.
        pid: The worker's process ID, which RAS reports for its rank.
    """

    world_rank: int
    node_id: str
    node_ip: str
    pid: int


class NCCLHangRun(abc.ABC):
    """The training run, as the NCCL hang detector's evaluators reach it."""

    @abc.abstractmethod
    def workers(self) -> List[WorkerInfo]:
        """The current attempt's workers, or none between attempts."""

    @abc.abstractmethod
    def upload(self, tool: str, files: Dict[str, str]) -> str:
        """Upload one diagnostic's files to the run's storage.

        Args:
            tool: The diagnostic, such as ``stack_traces``.
            files: ``{filename: contents}``.

        Returns:
            Where the files were uploaded, which outlives the cluster.
        """

    @abc.abstractmethod
    def record_state(self, state: NCCLHangDetectorState, stall_s: float) -> None:
        """Record the detector's state.

        Args:
            state: What the detector believes about the run.
            stall_s: How long the most stalled communicator has made no
                progress, in seconds.
        """


# ----------------------------------------------------------------------
# RAS reports
# ----------------------------------------------------------------------


@dataclass
class RASReport:
    """Structured summary of an ``ncclras`` JSON report.

    Attributes:
        timestamp: When ``ncclras`` responded.
        comm_op_counts: Maps each communicator id to ``{global_rank: {op_name:
            count}}`` for all of that communicator's ranks.
        comm_rank_status: Maps each communicator's ranks to their status. A
            hang requires that every rank is RUNNING.
        comm_rank_processes: Maps each communicator's ranks to the ``(host,
            pid)`` of the process running that rank, used to find the Ray Train
            worker behind a communicator rank.
        raw_json: The ``ncclras`` output this report was parsed from.
    """

    timestamp: str
    comm_op_counts: Dict[str, Dict[int, Dict[str, int]]]
    comm_rank_status: Dict[str, Dict[int, str]]
    comm_rank_processes: Dict[str, Dict[int, Tuple[str, int]]]
    raw_json: str

    @property
    def comm_op_skews(self) -> Dict[str, Dict[str, int]]:
        """Per-communicator, per-op spread (max-min) across its ranks.

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
        """The communicators whose ranks are all RUNNING and launched
        different numbers of an op."""
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
        """Whether no communicator is mismatched."""
        return not self.mismatched_comms

    @staticmethod
    def rank_status(status: Dict[str, Any]) -> str:
        """Convert a rank's status to a human-readable string."""
        # `async_error` is ignored by the upstream RAS implementation:
        # https://github.com/NVIDIA/nccl/blob/master/src/ras/client_support.cc
        if status["abort_flag"] is True:
            return "ABORT"
        if status["finalize_called"] is True or status["destroy_flag"] is True:
            return "FINALIZE"
        if status["init_state"] == 0:
            return "RUNNING"
        return "INIT"


def compute_report_op_diff(
    prev: RASReport, curr: RASReport
) -> Dict[str, Dict[int, Dict[str, int]]]:
    """Per-communicator, per-rank op-count delta between two consecutive reports.

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
            # NCCL 2.28.9 JSON ``missing_ranks[]`` has a missing comma, try
            # repair with regex.
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
            timestamp=data["timestamp"],
            comm_op_counts=comm_op_counts,
            comm_rank_status=comm_rank_status,
            comm_rank_processes=comm_rank_processes,
            raw_json=ras_json,
        )
    except (KeyError, TypeError, ValueError) as e:
        logger.info("NCCL RAS JSON did not match the expected schema: %s", e)
        return None


# ----------------------------------------------------------------------
# Naming the ranks
# ----------------------------------------------------------------------


def translate_ras_ranks_to_train(
    workers: Iterable[WorkerInfo], comm_rank_processes: Dict[int, Tuple[str, int]]
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
        by_process[(worker.node_ip, worker.pid)] = worker.world_rank
        by_pid[worker.pid].append(worker.world_rank)

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
    rank for ranks without a train worker. Consecutive ranks collapse into
    ranges, and each list is cut after ``_MAX_RANKS_LOGGED`` runs so a
    1000-rank job stays one line.

    Args:
        comm_ranks: Communicator ranks to name.
        train_rank_of: The translation from :func:`translate_ras_ranks_to_train`.

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


# ----------------------------------------------------------------------
# Evaluators
# ----------------------------------------------------------------------


class NCCLHangEvaluator(Evaluator):
    """Detects NCCL hangs in ``NCCLRASProbe``'s reports.

    A communicator is frozen when its ranks launched different numbers of an
    op and no rank advanced any op since the previous report. To confirm that
    this isn't a single-report blip, a communicator must stay frozen for
    consecutive reports, tracked as a per-communicator streak that any report
    in which it progressed resets. ``confirm_duration_s`` is converted to a
    number of reports with the poll interval.

    It warns once a communicator is suspected, then periodically, and logs
    NCCL's text report once per suspicion. A confirmed hang is diagnosed with
    the Flight Recorder (when ``TORCH_FR_BUFFER_SIZE`` is set), ``nvidia-smi``,
    the RAS text report and history, and the stack traces. A fatal ``ncclras``
    error, such as a missing binary, or an error in the detector, disables it
    for the rest of the run.

    Args:
        run: The run's workers, storage and metrics.
        poll_interval_s: How often ``NCCLRASProbe`` is polled.
        confirm_duration_s: How long a communicator stays frozen before its
            hang is confirmed.
        action: ``"fail"`` reattempts a confirmed hang, ``"observe"`` only logs
            it.
    """

    def __init__(
        self,
        run: NCCLHangRun,
        poll_interval_s: float,
        confirm_duration_s: float,
        action: str,
    ):
        self._run = run
        self._poll_interval_s = poll_interval_s
        self._confirm_duration_s = confirm_duration_s
        self._action = action

        # Escalation milestones, in polls
        self._confirm_poll_counts = math.ceil(confirm_duration_s / poll_interval_s)
        self._suspicion_polls = min(
            math.ceil(_FIRST_SUSPICION_AFTER_S / poll_interval_s),
            self._confirm_poll_counts - 1,
        )
        self._periodic_warn_polls = math.ceil(_PERIODIC_WARN_EVERY_S / poll_interval_s)

        # The previous report
        self.prev_report: Optional[RASReport] = None
        # The latest reports, saved with a confirmed hang's diagnostics.
        self.ras_history: Deque[RASReport] = deque(
            maxlen=self._confirm_poll_counts + _RAS_HISTORY_MARGIN_POLLS
        )
        # Per-communicator consecutive frozen-poll streaks ({comm_id: polls}).
        # As a deadlock requires the whole comm to be frozen (no op advancing),
        # any op progressing would indicate the comm overall isn't deadlocked.
        self.comm_deadlock_count: Dict[str, int] = {}
        # Whether NCCL's text report was logged since the latest new
        # suspicion, so it's logged once per suspicion, and fetched again on
        # the next warning if the fetch failed.
        self._ras_text_logged = False
        # The communicators whose confirmed hang was diagnosed. A Diagnose
        # returned while other on-demand checks run is dropped, so a confirmed
        # hang is diagnosed on each report until its diagnosis runs.
        self._diagnosed_comms: Set[str] = set()
        # The node whose report was evaluated last, and when that report was
        # taken, so each report is evaluated once.
        self._report_node: Optional[str] = None
        self._report_timestamp_s: Optional[float] = None
        # Disabled for the rest of the run, such as after a missing binary.
        self._is_ras_degraded = False

    def on_reset(self) -> None:
        self.prev_report = None
        self.ras_history.clear()
        self.reset_hang_counters()
        self._report_node = None
        self._report_timestamp_s = None
        self.record_detector_metrics()

    def reset_hang_counters(self) -> None:
        """Forget the frozen streaks, after a report with no mismatch."""
        self.comm_deadlock_count = {}
        self._diagnosed_comms = set()

    def evaluate(self, state: HealthState) -> HealthDecision:
        if self._is_ras_degraded:
            return Noop()

        # Detection must never fail the run.
        try:
            return self._evaluate(state)
        except Exception:  # noqa: BLE001
            logger.exception(
                "The Ray Train NCCL hang detector hit an unexpected error, therefore, "
                "disabling it for the rest of this training run."
            )
            self._is_ras_degraded = True
            self.record_detector_metrics()
            return Noop()

    def _evaluate(self, state: HealthState) -> HealthDecision:
        result = self.next_result(
            state.probe_results.get(NCCLRASProbe.probe_name(), {})
        )
        if result is None:
            # No new report since the last evaluation
            return Noop()
        if isinstance(result, Exception):
            logger.warning(
                "Disabling Ray Train NCCL hang detector for the rest of this run as "
                "`ncclras` returns %s.",
                result,
            )
            self._is_ras_degraded = True
            self.record_detector_metrics()
            return Noop()

        report = result
        self.ras_history.append(report)
        if report.mismatched_comms:
            decision = self.evaluate_comm_mismatch(report)
        else:  # Healthy with no mismatches, so every frozen streak is over
            decision = Noop()
            self.log_recovered_comms(frozen_counts={})
            self.reset_hang_counters()
            self.record_detector_metrics()
        self.prev_report = report
        return decision

    def next_result(
        self, results: Mapping[str, ProbeResult]
    ) -> Union[None, RASReport, Exception]:
        """Take the report to evaluate: the latest from the node evaluated
        last, or else from the first node that answered. Every node sees the
        whole RAS mesh, so one report per poll is enough.

        Args:
            results: ``NCCLRASProbe``'s latest result on each node.

        Returns:
            The new report, ``None`` if there is none, or the error that stops
            the detector when every node failed for good, such as a missing
            ``ncclras`` binary.
        """
        answered = {
            node: result
            for node, result in results.items()
            if result.error is None and result.detail
        }
        if not answered:
            errors = [result.error for result in results.values()]
            if errors and all(getattr(error, "fatal", False) for error in errors):
                return errors[0]
            # Until NCCL creates its first communicator there is no RAS
            # service to answer, so failed polls before the first report are
            # expected.
            return None

        node = self._report_node if self._report_node in answered else min(answered)
        result = answered[node]
        if node == self._report_node and result.timestamp_s == self._report_timestamp_s:
            return None
        self._report_node, self._report_timestamp_s = node, result.timestamp_s
        return parse_ras_schema(result.detail)

    def evaluate_comm_mismatch(self, report: RASReport) -> HealthDecision:
        """Advance the per-communicator frozen streaks and act on them.

        A communicator is deadlocked only when no rank advanced any op since
        the last poll: a real hang blocks every rank, so every op freezes. It's
        possible for an op mismatch to occur and NCCL continue which isn't
        detected currently.
        """
        if self.prev_report is None:
            self.record_detector_metrics()
            return Noop()

        # 1. Classify: which mismatched communicators made no progress this poll
        frozen_counts = self.compute_frozen_streaks(report)
        confirmed_comm_hangs = [
            comm_id
            for comm_id, count in frozen_counts.items()
            if count >= self._confirm_poll_counts
            and comm_id not in self._diagnosed_comms
        ]

        # 2. Update state: a communicator that progressed drops its streak
        self.log_recovered_comms(frozen_counts)
        self.comm_deadlock_count = frozen_counts
        self._diagnosed_comms &= set(frozen_counts)
        # Recorded before acting, as a confirmed hang spends its time
        # capturing diagnostics.
        self.record_detector_metrics()

        # 3. Act
        if confirmed_comm_hangs:
            return self.handle_confirmed_hangs(confirmed_comm_hangs, report)
        if frozen_counts:
            return self.handle_suspected_hangs(report)
        return Noop()

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

    def record_detector_metrics(self) -> None:
        """Record the detector's current state and the longest stall."""
        if self._is_ras_degraded:
            self._run.record_state(NCCLHangDetectorState.DEGRADED, 0)
            return

        max_streak = max(self.comm_deadlock_count.values(), default=0)
        if max_streak >= self._confirm_poll_counts:
            state = NCCLHangDetectorState.CONFIRMED
        elif max_streak >= max(self._suspicion_polls, 1):
            state = NCCLHangDetectorState.SUSPECTED
        else:
            state = NCCLHangDetectorState.HEALTHY
        self._run.record_state(state, max_streak * self._poll_interval_s)

    def log_recovered_comms(self, frozen_counts: Dict[str, int]) -> None:
        """Log each previously suspected communicator that is no longer frozen.

        Args:
            frozen_counts: This poll's streaks; anything in
                ``comm_deadlock_count`` but not here has resumed progress.
        """
        for comm_id, count in self.comm_deadlock_count.items():
            if comm_id not in frozen_counts and count >= self._suspicion_polls:
                logger.info(
                    "The Ray Train NCCL hang detector detects that the NCCL "
                    "communicator %s resumed making progress after being stalled "
                    "for %.0f seconds (%d polls). It is no longer suspected of hanging.",
                    comm_id,
                    count * self._poll_interval_s,
                    count,
                )

    def handle_confirmed_hangs(
        self, confirmed_comm_hangs: List[str], report: RASReport
    ) -> Diagnose:
        """Diagnose a confirmed hang, then act on it in ``HangDiagnostics``.

        Args:
            confirmed_comm_hangs: The communicators whose hang is confirmed.
            report: The report that confirmed it.

        Returns:
            The ``Diagnose`` that captures the diagnostics.
        """
        message = (
            "NCCL hang detected by the Ray Train NCCL hang detector! "
            f"{len(confirmed_comm_hangs)} communicator(s) made no collective "
            f"progress for {self._confirm_duration_s:.0f} seconds "
            f"({self._confirm_poll_counts} polls) while their ranks disagree on "
            "how many collectives they have launched.\n"
            f"{self.describe_stalled_comms(report, confirmed_comm_hangs)}\n"
            "Common causes are a rank taking a divergent code path or exiting "
            "early, ranks launching collectives in a different order or with "
            "mismatched shapes or dtypes, or a GPU or network failure.\n"
        )
        # The buffer has to be armed before the process group is created, so
        # an unarmed one holds no collectives.
        probes: List[Probe] = (
            [FlightRecorderProbe()]
            if TORCH_FR_BUFFER_SIZE_ENV_VAR in os.environ
            else []
        )
        probes += [NvidiaSmiProbe(), NCCLRASTextProbe(), StackTraceProbe()]
        diagnostics = HangDiagnostics(
            self._run,
            message,
            list(self.ras_history),
            self._action,
            on_done=lambda: self._diagnosed_comms.update(confirmed_comm_hangs),
        )
        return Diagnose(
            reason=(
                f"NCCL hang confirmed in {len(confirmed_comm_hangs)} "
                "communicator(s); capturing its diagnostics."
            ),
            checks=[
                HealthCheck(
                    probe_creator=lambda: probes,
                    evaluator_creator=lambda: [diagnostics],
                )
            ],
        )

    def handle_suspected_hangs(self, report: RASReport) -> HealthDecision:
        """Warn about a suspected hang, at its first suspicion and periodically
        after, and diagnose it with NCCL's text report once per suspicion.

        Args:
            report: The current poll's report.

        Returns:
            A ``Diagnose`` that logs NCCL's text report, or ``Noop()``.
        """
        # A communicator first warns at the suspicion threshold, then every
        # `_periodic_warn_polls` after it.
        new_suspicions = [
            comm_id
            for comm_id, count in self.comm_deadlock_count.items()
            if count == self._suspicion_polls
        ]
        if new_suspicions:
            # An earlier report predates these communicators stalling.
            self._ras_text_logged = False
            stalled_comms = new_suspicions
            headline = (
                "Possible NCCL hang detected by the Ray Train NCCL hang detector! "
                f"{len(new_suspicions)} communicator(s) have made no collective "
                "progress for "
                f"{self._suspicion_polls * self._poll_interval_s:.0f} seconds "
                "while their ranks disagree on how many collectives they have "
                "launched. Continuing to monitor, this might be a transient stall."
            )
        elif any(
            count > self._suspicion_polls
            and (count - self._suspicion_polls) % self._periodic_warn_polls == 0
            for count in self.comm_deadlock_count.values()
        ):
            stalled_comms = list(self.comm_deadlock_count)
            headline = (
                "NCCL hang still suspected by the Ray Train NCCL hang detector! "
                f"{len(stalled_comms)} communicator(s) have made no progress."
            )
        else:
            return Noop()

        if self._action == NCCL_HANG_ACTION_FAIL:
            remaining_polls = self._confirm_poll_counts - max(
                self.comm_deadlock_count.values()
            )
            headline += (
                " The run will be reattempted in "
                f"{remaining_polls * self._poll_interval_s:.0f} seconds "
                f"({remaining_polls} more polls) if this persists."
            )
        logger.warning(
            "%s\n%s", headline, self.describe_stalled_comms(report, stalled_comms)
        )

        # NCCL's own view of the hang, once per suspected hang; it is saved
        # with the diagnostics if the hang is confirmed.
        if self._ras_text_logged:
            return Noop()
        return Diagnose(
            reason="NCCL hang suspected; fetching NCCL's `ncclras` report.",
            checks=[
                HealthCheck(
                    probe_creator=lambda: [NCCLRASTextProbe()],
                    evaluator_creator=lambda: [
                        LogRASTextReport(on_logged=self._mark_ras_text_logged)
                    ],
                )
            ],
        )

    def _mark_ras_text_logged(self) -> None:
        self._ras_text_logged = True

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
        disable detection.

        Args:
            report: The current poll's report.
            stalled_comm_ids: The stalled communicators to describe.

        Returns:
            One line per communicator, or ``""`` if they couldn't be built.
        """
        try:
            workers = self._run.workers()
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
                stalled_s = (
                    self.comm_deadlock_count.get(comm_id, 0) * self._poll_interval_s
                )
                described_stalls.append(
                    f"  - Communicator {comm_id} over {comm_ranks}, "
                    f"no progress for {stalled_s:.0f}s. {'. '.join(ops)}"
                )
            return "\n".join(described_stalls)

        except Exception:  # noqa: BLE001
            logger.debug("Could not describe the stalled communicators.", exc_info=True)
            return ""


def _first_detail(
    results: Mapping[Any, ProbeResult]
) -> Tuple[Optional[str], Optional[Exception]]:
    """The first answer among a probe's results, by key.

    Args:
        results: The probe's result for each worker or node.

    Returns:
        ``(detail, None)`` for the first result with a detail, or ``(None,
        error)`` with the last error if none has one.
    """
    error = None
    for _, result in sorted(results.items()):
        if result.error is None and result.detail:
            return result.detail, None
        error = result.error
    return None, error


class LogRASTextReport(Evaluator):
    """Logs NCCL's text report of a suspected hang.

    Args:
        on_logged: Called once the report is logged.
    """

    def __init__(self, on_logged: Callable[[], None]):
        self._on_logged = on_logged

    def evaluate(self, state: HealthState) -> HealthDecision:
        text, error = _first_detail(
            state.probe_results.get(NCCLRASTextProbe.probe_name(), {})
        )
        if text is None:
            logger.info("Could not fetch the `ncclras` text report (%s).", error)
            return Noop()
        logger.info(
            "NCCL's own `ncclras` report of the suspected hang (logged once per "
            "hang). Its ranks are numbered per communicator, not by Ray Train "
            "world rank:\n%s",
            text.rstrip(),
        )
        self._on_logged()
        return Noop()


class HangDiagnostics(Evaluator):
    """Uploads a confirmed hang's diagnostics, then reattempts (``fail``) or
    only logs the hang (``observe``).

    Every rank (``nvidia-smi``: every node) gets a file, holding its
    diagnostic, or why there is none.

    Args:
        run: The run's workers and storage.
        message: What the detector saw.
        ras_history: The latest RAS reports.
        action: ``"fail"`` or ``"observe"``.
        on_done: Called once the diagnostics are evaluated.
    """

    def __init__(
        self,
        run: NCCLHangRun,
        message: str,
        ras_history: List[RASReport],
        action: str,
        on_done: Callable[[], None],
    ):
        self._run = run
        self._message = message
        self._ras_history = ras_history
        self._action = action
        self._on_done = on_done

    def evaluate(self, state: HealthState) -> HealthDecision:
        self._on_done()
        results = state.probe_results
        flight_recorder_dir = capture_diagnostic(
            "Flight Recorder dumps",
            lambda: self.upload_flight_recorder(
                results.get(FlightRecorderProbe.probe_name(), {})
            ),
        )
        nvidia_smi_dir = capture_diagnostic(
            "nvidia-smi snapshots",
            lambda: self.upload_nvidia_smi(
                results.get(NvidiaSmiProbe.probe_name(), {})
            ),
        )
        ras_history_dir = capture_diagnostic(
            "`ncclras` query history",
            lambda: self.upload_ras_history(
                results.get(NCCLRASTextProbe.probe_name(), {})
            ),
        )
        stack_trace_dir = capture_diagnostic(
            "worker stack traces",
            lambda: self.upload_stack_traces(
                results.get(StackTraceProbe.probe_name(), {})
            ),
        )

        message = self._message
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

        if self._action == NCCL_HANG_ACTION_FAIL:
            return NCCLHangReattempt(reason=message)
        logger.warning(message)
        return Noop()

    def upload_flight_recorder(
        self, results: Mapping[int, ProbeResult]
    ) -> Optional[str]:
        """Upload each rank's Flight Recorder buffer as ``rank_<rank>.json``.

        Args:
            results: ``FlightRecorderProbe``'s result on each rank.

        Returns:
            Where the dumps were uploaded, or ``None`` if there are none.
        """
        files: Dict[str, str] = {}
        for rank, result in sorted(results.items()):
            if result.error is not None:
                logger.info(
                    "No Flight Recorder dump from rank %d: %s", rank, result.error
                )
                # Still valid JSON, so every file in the directory parses.
                files[f"rank_{rank}.json"] = json.dumps(
                    {"dump_error": str(result.error)}
                )
                continue
            files[f"rank_{rank}.json"] = result.detail or ""
        return self._run.upload(FLIGHT_RECORDER_TOOL, files) if files else None

    def upload_nvidia_smi(self, results: Mapping[str, ProbeResult]) -> Optional[str]:
        """Upload each node's GPU snapshot as ``node_<node ip>.log``.

        Args:
            results: ``NvidiaSmiProbe``'s result on each node.

        Returns:
            Where the snapshots were uploaded, or ``None`` if there are none.
        """
        node_ips = {worker.node_id: worker.node_ip for worker in self._run.workers()}
        files = {
            f"node_{node_ips.get(node_id, node_id)}.log": (
                result.detail
                if result.error is None
                else f"no `nvidia-smi` snapshot: {result.error}"
            )
            or ""
            for node_id, result in sorted(results.items())
        }
        return self._run.upload(NVIDIA_SMI_TOOL, files) if files else None

    def upload_ras_history(
        self, text_results: Mapping[str, ProbeResult]
    ) -> Optional[str]:
        """Upload the latest RAS reports as ``ncclras_<timestamp>.json``, and
        NCCL's text report as ``ncclras_report.txt``.

        Args:
            text_results: ``NCCLRASTextProbe``'s result on each node.

        Returns:
            Where the history was uploaded, or ``None`` if there is none.
        """
        files = {
            f"ncclras_{_UNSAFE_FILENAME_CHARS.sub('-', report.timestamp)}.json": report.raw_json
            for report in self._ras_history
        }
        text, error = _first_detail(text_results)
        if text:
            files["ncclras_report.txt"] = text
        elif error is not None:
            logger.info("Could not fetch the `ncclras` text report (%s).", error)
        return self._run.upload(NCCL_RAS_TOOL, files) if files else None

    def upload_stack_traces(self, results: Mapping[int, ProbeResult]) -> Optional[str]:
        """Upload each rank's stack traces as ``rank_<rank>.log``.

        Args:
            results: ``StackTraceProbe``'s result on each rank.

        Returns:
            Where the stack traces were uploaded, or ``None`` if there are none.
        """
        files = {
            f"rank_{rank}.log": (
                result.detail if result.error is None else str(result.error)
            )
            or ""
            for rank, result in sorted(results.items())
        }
        return self._run.upload(STACK_TRACES_TOOL, files) if files else None


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


def nccl_hang_check(
    run: NCCLHangRun,
    poll_interval_s: float = 15.0,
    confirm_duration_s: float = 600.0,
    action: str = NCCL_HANG_ACTION_OBSERVE,
) -> PeriodicHealthCheck:
    """The NCCL hang detector's in-flight check.

    Args:
        run: The run's workers, storage and metrics.
        poll_interval_s: How often to query NCCL RAS.
        confirm_duration_s: How long a communicator stays frozen before its
            hang is confirmed.
        action: ``"fail"`` reattempts a confirmed hang, ``"observe"`` only logs
            it.

    Returns:
        The check.

    Raises:
        ValueError: If a duration isn't positive, or the action is unknown.
    """
    if poll_interval_s <= 0:
        raise ValueError(f"poll_interval_s must be positive, got {poll_interval_s}.")
    if confirm_duration_s <= 0:
        raise ValueError(
            f"confirm_duration_s must be positive, got {confirm_duration_s}."
        )
    if action not in (NCCL_HANG_ACTION_FAIL, NCCL_HANG_ACTION_OBSERVE):
        raise ValueError(
            f"action must be one of {NCCL_HANG_ACTION_FAIL!r} or "
            f"{NCCL_HANG_ACTION_OBSERVE!r}, got {action!r}."
        )
    return PeriodicHealthCheck(
        probe_creator=lambda: [NCCLRASProbe()],
        evaluator_creator=lambda: [
            NCCLHangEvaluator(run, poll_interval_s, confirm_duration_s, action)
        ],
        poll_interval_s=poll_interval_s,
    )
