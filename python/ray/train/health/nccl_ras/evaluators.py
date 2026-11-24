"""Evaluators of the NCCL hang detector.

``NCCLHangEvaluator`` evaluates ``NCCLRASProbe``'s periodic reports. On a
suspected hang, it returns a ``Diagnose`` with ``NCCLRASTextProbe``, which
``SuspectedHangEvaluator`` evaluates. On a confirmed hang, it returns a
``Diagnose`` with four probes, which ``ConfirmedHangEvaluator`` evaluates: it
uploads their results, then reattempts the run or only logs the hang.
"""
import json
import logging
import math
import os
import tempfile
from collections import deque
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from ray.train.health.check import HealthCheck
from ray.train.health.decision import Diagnose, HealthDecision, Noop, Reattempt
from ray.train.health.evaluator import Evaluator
from ray.train.health.nccl_ras.constants import (
    FIRST_SUSPICION_AFTER_S,
    FLIGHT_RECORDER_TOOL,
    GRAFANA_STALL_DURATION_S,
    GRAFANA_STATE,
    GRAFANA_STATE_TAG_KEY,
    NCCL_RAS_TOOL,
    NVIDIA_SMI_TOOL,
    PERIODIC_WARN_EVERY_S,
    RAS_HISTORY_MARGIN_POLLS,
    STACK_TRACES_TOOL,
    UNSAFE_FILENAME_CHARS,
    NCCLHangDetectorState,
)
from ray.train.health.nccl_ras.probes import (
    FlightRecorderProbe,
    NCCLRASProbe,
    NCCLRASTextProbe,
    NvidiaSmiProbe,
    StackTraceProbe,
)
from ray.train.health.nccl_ras.utils import (
    RASReport,
    compute_report_op_diff,
    format_ranks,
    parse_ras_schema,
    translate_ras_ranks_to_train,
)
from ray.train.health.probe import Probe, ProbeResult
from ray.train.health.state import HealthState
from ray.train.v2._internal.constants import (
    HANG_DETECTOR_DIRNAME,
    NCCL_RAS_ACTION_FAIL,
    TORCH_FR_BUFFER_SIZE_ENV_VAR,
)
from ray.train.v2._internal.execution.storage import _upload_to_fs_path
from ray.train.v2._internal.metrics.base import (
    RUN_ID_TAG_KEY,
    RUN_NAME_TAG_KEY,
    EnumMetric,
    Metric,
    ValueMetric,
)

if TYPE_CHECKING:
    from ray.train.v2._internal.execution.context import TrainRunContext
    from ray.train.v2._internal.execution.storage import StorageContext
    from ray.train.v2._internal.execution.worker_group import Worker, WorkerGroup

logger = logging.getLogger(__name__)


# --------------------------
# Periodic: NCCLRASProbe's reports
# --------------------------


class NCCLHangEvaluator(Evaluator):
    """Detects NCCL hangs in ``NCCLRASProbe``'s reports.

    A communicator is frozen when its ranks launched different numbers of an
    op and no rank advanced any op since the previous report. Its hang is
    confirmed once it stays frozen for ``confirm_duration_s``, counted in
    consecutive reports; any report in which it progressed resets the count.

    It warns once a communicator is suspected, then periodically. A suspected
    hang returns a ``Diagnose`` that logs NCCL's text report once per
    suspicion, and a confirmed hang a ``Diagnose`` that captures its
    diagnostics. Each report records the detector's state and the longest
    stall as Grafana metrics, which are cleared at the end of each attempt.
    A fatal ``ncclras`` error, such as a missing binary, or an error in the
    detector disables it for the rest of the run.

    Args:
        poll_interval_s: How often ``NCCLRASProbe`` is polled.
        confirm_duration_s: How long a communicator stays frozen before its
            hang is confirmed.
        action: ``"fail"`` reattempts a confirmed hang, ``"observe"`` only logs
            it.
        train_run_context: The run, which tags the metrics and stores the
            diagnostics.
        get_worker_group: Returns the attempt's worker group, whose workers
            name the ranks.
    """

    def __init__(
        self,
        poll_interval_s: float,
        confirm_duration_s: float,
        action: str,
        train_run_context: "TrainRunContext",
        get_worker_group: Callable[[], Optional["WorkerGroup"]],
    ):
        self._poll_interval_s = poll_interval_s
        self._confirm_duration_s = confirm_duration_s
        self._action = action
        self._storage_context = train_run_context.run_config.storage_context
        self._get_worker_group = get_worker_group

        # Escalation milestones, in polls
        self._confirm_poll_counts = math.ceil(confirm_duration_s / poll_interval_s)
        self._suspicion_polls = min(
            math.ceil(FIRST_SUSPICION_AFTER_S / poll_interval_s),
            self._confirm_poll_counts - 1,
        )
        self._periodic_warn_polls = math.ceil(PERIODIC_WARN_EVERY_S / poll_interval_s)

        base_tags = {
            RUN_NAME_TAG_KEY: train_run_context.get_run_config().name,
            RUN_ID_TAG_KEY: train_run_context.run_id,
        }
        self._metrics: Dict[str, Metric] = {
            GRAFANA_STATE: EnumMetric[NCCLHangDetectorState](
                name=GRAFANA_STATE,
                description="Current state of the NCCL hang detector.",
                base_tags=base_tags,
                enum_tag_key=GRAFANA_STATE_TAG_KEY,
            ),
            GRAFANA_STALL_DURATION_S: ValueMetric(
                name=GRAFANA_STALL_DURATION_S,
                description=(
                    "Seconds the most stalled NCCL communicator has made no "
                    "progress with a collective mismatch."
                ),
                base_tags=base_tags,
            ),
        }

        # The previous report
        self.prev_report: Optional[RASReport] = None
        # The latest reports, saved with a confirmed hang's diagnostics.
        self.ras_history: Deque[RASReport] = deque(
            maxlen=self._confirm_poll_counts + RAS_HISTORY_MARGIN_POLLS
        )
        # Per-communicator consecutive frozen-poll streaks ({comm_id: polls}).
        # A deadlock freezes the whole communicator, so any op that advanced
        # means it isn't deadlocked.
        self.comm_deadlock_count: Dict[str, int] = {}
        # Whether NCCL's text report was logged since the latest new
        # suspicion: it's logged once per suspicion, and fetched again on the
        # next warning if the fetch failed.
        self._ras_text_logged = False
        # The communicators whose confirmed hang was diagnosed.
        self._diagnosed_comms: Set[str] = set()
        # The Diagnose of a confirmed hang that hasn't run yet. The
        # HealthManager drops a Diagnose returned while other on-demand checks
        # run, so it's returned on every evaluation until it runs.
        self._pending_diagnose: Optional[Diagnose] = None
        # The node whose report was evaluated last, and when that report was
        # taken, so each report is evaluated once.
        self._report_node: Optional[str] = None
        self._report_timestamp_s: Optional[float] = None
        # When each node's latest report that didn't parse was taken, so that
        # it's parsed once.
        self._unparsed_timestamps_s: Dict[str, float] = {}
        # When the latest poll that no node answered was taken, so that it's
        # logged once.
        self._failed_poll_timestamp_s: Optional[float] = None
        # Whether a fatal error that didn't disable detection was logged.
        self._fatal_error_logged = False
        # Disabled for the rest of the run, such as after a missing binary.
        self._is_ras_degraded = False

    def on_reset(self) -> None:
        self.prev_report = None
        self.ras_history.clear()
        self.reset_hang_counters()
        self._report_node = None
        self._report_timestamp_s = None
        self._unparsed_timestamps_s = {}
        self.record_detector_metrics()

    def on_shutdown(self) -> None:
        # Clear the gauges so a finished run doesn't keep showing its last state.
        for metric in self._metrics.values():
            metric.reset()

    def reset_hang_counters(self) -> None:
        """Per-healthy-poll debounce reset `_prev_report` left for the next poll as comparison."""
        self.comm_deadlock_count = {}
        self._diagnosed_comms = set()
        self._pending_diagnose = None

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
            if self._pending_diagnose is not None:
                return self._pending_diagnose
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

        self.ras_history.append(result)
        if result.mismatched_comms:
            decision = self.evaluate_comm_mismatch(result)
        else:  # Healthy with no mismatches, so every frozen streak is over
            decision = Noop()
            self.log_recovered_comms(frozen_counts={})
            self.reset_hang_counters()
            self.record_detector_metrics()
        self.prev_report = result
        return decision

    def next_result(
        self, results: Mapping[str, ProbeResult]
    ) -> Union[None, RASReport, Exception]:
        """Take the report to evaluate: the latest from the node evaluated
        last, or else from the first other node whose report parses, as the
        callback tried the next worker. Every node sees the whole RAS mesh, so
        one report per poll is enough.

        Args:
            results: ``NCCLRASProbe``'s latest result on each node.

        Returns:
            The new report, ``None`` if there is none, or the error that stops
            the detector when every worker node failed for good, such as with
            a missing ``ncclras`` binary.
        """
        answered = {
            node: result
            for node, result in results.items()
            if result.error is None and result.detail
        }
        if not answered:
            errors = [result.error for result in results.values()]
            if errors and all(getattr(error, "fatal", False) for error in errors):
                # Like the callback, which tried every worker, wait for every
                # worker node: one that failed first can't stop the detector
                # alone. A node whose NodeMonitor failed never reports, so
                # warn meanwhile.
                worker_nodes = {worker.metadata.node_id for worker in self._workers()}
                if worker_nodes <= results.keys():
                    return errors[0]
                if not self._fatal_error_logged:
                    self._fatal_error_logged = True
                    logger.warning(
                        "The Ray Train NCCL hang detector can't detect hangs "
                        "while `ncclras` fails on every node that answered (%s).",
                        errors[0],
                    )
            # Until NCCL creates its first communicator there is no RAS
            # service to answer, so failed polls before the first report are
            # expected, and only logged at DEBUG.
            failed = max(
                results.values(), key=lambda r: r.timestamp_s or 0.0, default=None
            )
            if failed and failed.timestamp_s != self._failed_poll_timestamp_s:
                self._failed_poll_timestamp_s = failed.timestamp_s
                logger.log(
                    logging.INFO if self.prev_report is not None else logging.DEBUG,
                    "`ncclras` poll produced no report (%s). Will retry next poll.",
                    failed.error,
                )
            return None

        # The node evaluated last first, then the others in order.
        for node in sorted(answered, key=lambda n: (n != self._report_node, n)):
            result = answered[node]
            if (
                node == self._report_node
                and result.timestamp_s == self._report_timestamp_s
            ):
                return None
            if self._unparsed_timestamps_s.get(node) == result.timestamp_s:
                continue
            report = parse_ras_schema(result.detail)
            if report is None:
                self._unparsed_timestamps_s[node] = result.timestamp_s
                continue
            self._report_node, self._report_timestamp_s = node, result.timestamp_s
            return report
        return None

    def evaluate_comm_mismatch(self, report: RASReport) -> HealthDecision:
        """Advance the per-communicator frozen streaks and act on them.

        A communicator is deadlocked only when *no* rank advanced *any* op since
        the last poll: a real hang blocks every rank, so every op freezes. It's
        possible for an op mismatch to occur and NCCL continue which isn't
        detected currently.

        Args:
            report: The current poll's report.

        Returns:
            A ``Diagnose`` of a confirmed or newly suspected hang, or
            ``Noop()``.
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
        self._pending_diagnose = None
        # Recorded before acting, as a confirmed hang can reattempt the run.
        self.record_detector_metrics()

        # 3. Act
        if confirmed_comm_hangs:
            return self.handle_confirmed_hangs(confirmed_comm_hangs, report)
        elif frozen_counts:
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

    def record_detector_metrics(self):
        """Record the detector's current state and the longest stall."""
        if self._is_ras_degraded:
            self._metrics[GRAFANA_STALL_DURATION_S].record(0)
            self._metrics[GRAFANA_STATE].record(NCCLHangDetectorState.DEGRADED)
            return

        max_streak = max(self.comm_deadlock_count.values(), default=0)
        self._metrics[GRAFANA_STALL_DURATION_S].record(
            max_streak * self._poll_interval_s
        )

        if max_streak >= self._confirm_poll_counts:
            self._metrics[GRAFANA_STATE].record(NCCLHangDetectorState.CONFIRMED)
        elif max_streak >= max(self._suspicion_polls, 1):
            self._metrics[GRAFANA_STATE].record(NCCLHangDetectorState.SUSPECTED)
        else:
            self._metrics[GRAFANA_STATE].record(NCCLHangDetectorState.HEALTHY)

    def log_recovered_comms(self, frozen_counts: Dict[str, int]):
        """Log each previously suspected communicator that is no longer frozen.

        Args:
            frozen_counts: This poll's streaks; anything in
                ``comm_deadlock_count`` but not here has resumed progress.
        """
        for comm_id, count in self.comm_deadlock_count.items():
            if comm_id not in frozen_counts and count >= self._suspicion_polls:
                logger.info(
                    "The Ray Train NCCL hang detector detects that the "
                    "NCCL communicator %s resumed making progress after being stalled "
                    "for %.0f seconds (%d polls). It is no longer suspected of hanging.",
                    comm_id,
                    count * self._poll_interval_s,
                    count,
                )

    def handle_confirmed_hangs(
        self, confirmed_comm_hangs: List[str], report: RASReport
    ) -> Diagnose:
        """Diagnose a confirmed hang, which ``ConfirmedHangEvaluator`` then acts on.

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
        diagnostics = ConfirmedHangEvaluator(
            self._storage_context,
            {w.metadata.node_id: w.metadata.node_ip for w in self._workers()},
            message,
            list(self.ras_history),
            self._action,
            on_done=lambda: self._on_diagnosed(confirmed_comm_hangs),
        )
        self._pending_diagnose = Diagnose(
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
        return self._pending_diagnose

    def _on_diagnosed(self, comm_ids: List[str]) -> bool:
        """Called once a confirmed hang's diagnostics are back. Marks its
        communicators that are still hung as diagnosed, so that their hang
        isn't diagnosed again. One that made progress since can be confirmed
        again.

        Args:
            comm_ids: The communicators whose hang was confirmed.

        Returns:
            Whether any of them is still hung.
        """
        self._pending_diagnose = None
        still_hung = [
            comm_id
            for comm_id in comm_ids
            if self.comm_deadlock_count.get(comm_id, 0) >= self._confirm_poll_counts
        ]
        self._diagnosed_comms.update(still_hung)
        return bool(still_hung)

    def handle_suspected_hangs(self, report: RASReport) -> HealthDecision:
        # A communicator first warns at the suspicion threshold, then every
        # `_periodic_warn_polls` after it.
        new_suspicions = [
            comm_id
            for comm_id, count in self.comm_deadlock_count.items()
            if count == self._suspicion_polls
        ]
        if new_suspicions:
            # TODO: Add a Grafana annotation with
            #  `ray.train.annotate(..., severity="warning")` once annotations
            #  are merged.
            # An earlier report predates these communicators stalling.
            self._ras_text_logged = False
            stalled_comms = new_suspicions
            headline = (
                f"Possible NCCL hang detected by the Ray Train NCCL hang detector! "
                f"{len(new_suspicions)} communicator(s) have made no collective progress "
                f"for {self._suspicion_polls * self._poll_interval_s:.0f} seconds "
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

        if self._action == NCCL_RAS_ACTION_FAIL:
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
                        SuspectedHangEvaluator(on_logged=self._mark_ras_text_logged)
                    ],
                )
            ],
        )

    def _mark_ras_text_logged(self) -> None:
        self._ras_text_logged = True

    def _workers(self) -> List["Worker"]:
        worker_group = self._get_worker_group()
        return worker_group.get_workers() if worker_group else []

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
        reach the evaluator and disable detection.

        Args:
            report: The current poll's report.
            stalled_comm_ids: The stalled communicators to describe.

        Returns:
            One line per communicator, or ``""`` if they couldn't be built.
        """

        try:
            workers = self._workers()
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


# --------------------------
# On demand: the Diagnoses of a suspected and a confirmed hang
# --------------------------


def _first_detail(
    results: Mapping[Any, ProbeResult]
) -> Tuple[Optional[str], Optional[Exception]]:
    """The first answer among a probe's results, by key.

    Args:
        results: The probe's result on each worker or node.

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


class SuspectedHangEvaluator(Evaluator):
    """Evaluates the ``Diagnose`` of a suspected hang: logs NCCL's text report,
    from ``NCCLRASTextProbe``. Training goes on.

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


class ConfirmedHangEvaluator(Evaluator):
    """Evaluates the ``Diagnose`` of a confirmed hang: uploads the results of
    its four probes to ``<experiment_fs_path>/hang_detector/<tool>/``, then
    reattempts the run (``fail``) or only logs the hang (``observe``). If
    every one of its communicators made progress since, it still uploads
    them, but only logs that.

    Every rank (``nvidia-smi``: every node) gets a file, holding its
    diagnostic, or why there is none.

    Args:
        storage_context: The run's storage.
        node_ips: ``{node ID: node IP}`` of the attempt's workers, which name
            the ``nvidia-smi`` files.
        message: What the detector saw.
        ras_history: The latest RAS reports.
        action: ``"fail"`` or ``"observe"``.
        on_done: Called once the diagnostics are back. Returns whether any of
            the communicators is still hung.
    """

    def __init__(
        self,
        storage_context: "StorageContext",
        node_ips: Dict[str, str],
        message: str,
        ras_history: List[RASReport],
        action: str,
        on_done: Callable[[], bool],
    ):
        self._storage_context = storage_context
        self._node_ips = node_ips
        self._message = message
        self._ras_history = ras_history
        self._action = action
        self._on_done = on_done

    def evaluate(self, state: HealthState) -> HealthDecision:
        still_hung = self._on_done()
        results = state.probe_results
        flight_recorder_dir = self.upload_flight_recorder(
            results.get(FlightRecorderProbe.probe_name(), {})
        )
        nvidia_smi_dir = self.upload_nvidia_smi(
            results.get(NvidiaSmiProbe.probe_name(), {})
        )
        ras_history_dir = self.upload_ras_history(
            results.get(NCCLRASTextProbe.probe_name(), {})
        )
        stack_trace_dir = self.upload_stack_traces(
            results.get(StackTraceProbe.probe_name(), {})
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

        if not still_hung:
            logger.warning(
                "%sThe communicator(s) resumed making progress while these "
                "diagnostics were captured.",
                message,
            )
            return Noop()
        if self._action == NCCL_RAS_ACTION_FAIL:
            return Reattempt(reason=message)
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
            else:
                files[f"rank_{rank}.json"] = result.detail or ""
        return self.upload(FLIGHT_RECORDER_TOOL, files)

    def upload_nvidia_smi(self, results: Mapping[str, ProbeResult]) -> Optional[str]:
        """Upload each node's GPU snapshot as ``node_<node ip>.log``.

        Args:
            results: ``NvidiaSmiProbe``'s result on each node.

        Returns:
            Where the snapshots were uploaded, or ``None`` if there are none.
        """
        files = {
            f"node_{self._node_ips.get(node_id, node_id)}.log": (
                result.detail
                if result.error is None
                else f"no `nvidia-smi` snapshot: {result.error}"
            )
            or ""
            for node_id, result in sorted(results.items())
        }
        return self.upload(NVIDIA_SMI_TOOL, files)

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
            f"ncclras_{UNSAFE_FILENAME_CHARS.sub('-', report.timestamp)}.json": report.raw_json
            for report in self._ras_history
        }
        text, error = _first_detail(text_results)
        if text:
            files["ncclras_report.txt"] = text
        elif error is not None:
            logger.info("Could not fetch the `ncclras` text report (%s).", error)
        return self.upload(NCCL_RAS_TOOL, files)

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
        return self.upload(STACK_TRACES_TOOL, files)

    def upload(self, tool: str, files: Dict[str, str]) -> Optional[str]:
        """Upload one diagnostic's files to the run's storage. A failed upload
        is logged, so that it loses no other diagnostic.

        Args:
            tool: The diagnostic, such as ``stack_traces``.
            files: ``{filename: contents}``.

        Returns:
            Where the files were uploaded, which outlives the cluster, or
            ``None`` if there are none or the upload failed.
        """
        if not files:
            return None
        fs_path = os.path.join(
            self._storage_context.experiment_fs_path, HANG_DETECTOR_DIRNAME, tool
        )
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                for name, contents in files.items():
                    (Path(temp_dir) / name).write_text(contents)
                _upload_to_fs_path(
                    temp_dir, self._storage_context.storage_filesystem, fs_path
                )
        except Exception:  # noqa: BLE001
            logger.exception("Uploading the %s diagnostics failed.", tool)
            return None
        return fs_path
