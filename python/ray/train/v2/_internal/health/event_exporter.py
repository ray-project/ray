import logging
import time
import uuid
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from ray._common.observability.config import is_python_ray_event_enabled
from ray.core.generated.events_health_event_pb2 import (
    HealthCheckerDisabled,
    HealthDecision,
    HealthEvent,
    HealthFinding,
    NcclHangFinding,
)

logger = logging.getLogger(__name__)

HEALTH_EVENT_TYPE = "HEALTH_EVENT"

# The checker_name the HealthManager's own decisions are emitted under.
HEALTH_MANAGER_CHECKER_NAME = "health_manager"

# Text artifacts are capped so one event can't flood the export pipeline.
MAX_ARTIFACT_TEXT_BYTES = 32 * 1024

FindingDetails = Union[NcclHangFinding, Dict[str, Any]]


class HealthEventExporter:
    """Exports the health plane's findings and decisions as HEALTH_EVENT Ray events.

    The exporter owns the bookkeeping that the HealthEvent contract in
    `events_health_event.proto` requires, so individual checkers can't break it:

    - `sequence` is strictly increasing per checker.
    - A finding is NEW once, then UPDATED, and ends RESOLVED or STOPPED. Its
      finding_id emits nothing after that; if the issue recurs, it's reported
      as a new finding with a new finding_id.
    - Every finding event is a full snapshot, with artifacts only on the event
      that produced them.
    - A checker emits nothing after it is disabled. Its open findings are
      STOPPED first, so consumers see every finding close.

    Events are only exported when `RAY_enable_ray_event` is on and
    `HEALTH_EVENT` is listed in `RAY_ENABLE_PYTHON_RAY_EVENT_TYPES`; the
    bookkeeping runs either way. Invalid arguments raise, but export failures
    are only logged so they can't fail the training run. Not thread-safe: call
    it from the controller's event loop.
    """

    def __init__(self):
        self._enabled = is_python_ray_event_enabled(HEALTH_EVENT_TYPE)
        self._sequences: Dict[str, int] = defaultdict(int)
        # finding_id -> (checker_name, the finding's latest snapshot).
        self._open_findings: Dict[str, Tuple[str, HealthFinding]] = {}
        self._disabled_checkers: Set[str] = set()

    def open_finding_ids(self, checker_name: Optional[str] = None) -> List[str]:
        """The IDs of findings that haven't been resolved or stopped."""
        return [
            finding_id
            for finding_id, (checker, _) in self._open_findings.items()
            if checker_name is None or checker == checker_name
        ]

    def emit_new_finding(
        self,
        checker_name: str,
        *,
        summary: str,
        started_at: float,
        details: Optional[FindingDetails] = None,
        artifacts: Sequence[HealthFinding.HealthArtifact] = (),
        observed_at: Optional[float] = None,
    ) -> Optional[str]:
        """Reports a new finding, including a recurrence of a closed one.

        Args:
            checker_name: The checker reporting the finding.
            summary: Short human-readable summary.
            started_at: When the underlying issue began, in seconds since the
                epoch.
            details: An NcclHangFinding, or a JSON-serializable dict for a user
                checker.
            artifacts: Evidence for the finding. Text is capped at 32 KB.
            observed_at: When the observation was made, in seconds since the
                epoch. Defaults to now.

        Returns:
            The generated finding ID, or None if the checker is disabled.
        """
        if checker_name in self._disabled_checkers:
            logger.warning(
                f"Dropping finding from disabled health checker '{checker_name}'."
            )
            return None
        finding = HealthFinding(finding_id=uuid.uuid4().hex, summary=summary)
        finding.started_at.FromNanoseconds(int(started_at * 1e9))
        self._report(
            checker_name,
            finding,
            HealthFinding.State.NEW,
            details=details,
            artifacts=artifacts,
            observed_at=observed_at,
        )
        return finding.finding_id

    def emit_updated_finding(
        self,
        finding_id: str,
        *,
        summary: Optional[str] = None,
        details: Optional[FindingDetails] = None,
        artifacts: Sequence[HealthFinding.HealthArtifact] = (),
        observed_at: Optional[float] = None,
    ) -> None:
        """Reports a change to an open finding.

        Args:
            finding_id: The open finding.
            summary: Replaces the latest summary if given.
            details: Replaces the latest details if given.
            artifacts: Evidence produced since the last event.
            observed_at: When the observation was made, in seconds since the
                epoch. Defaults to now.
        """
        self._report_open(
            finding_id,
            HealthFinding.State.UPDATED,
            summary=summary,
            details=details,
            artifacts=artifacts,
            observed_at=observed_at,
        )

    def emit_resolved_finding(
        self,
        finding_id: str,
        *,
        summary: Optional[str] = None,
        details: Optional[FindingDetails] = None,
        artifacts: Sequence[HealthFinding.HealthArtifact] = (),
        observed_at: Optional[float] = None,
    ) -> None:
        """Reports that an open finding's issue cleared on its own.

        Args:
            finding_id: The open finding.
            summary: Replaces the latest summary if given.
            details: Replaces the latest details if given, e.g. an
                NcclHangFinding with no communicators once all recover.
            artifacts: Evidence produced since the last event.
            observed_at: When the observation was made, in seconds since the
                epoch. Defaults to now.
        """
        self._report_open(
            finding_id,
            HealthFinding.State.RESOLVED,
            summary=summary,
            details=details,
            artifacts=artifacts,
            observed_at=observed_at,
        )

    def emit_stopped_finding(
        self,
        finding_id: str,
        *,
        summary: Optional[str] = None,
        details: Optional[FindingDetails] = None,
        artifacts: Sequence[HealthFinding.HealthArtifact] = (),
        observed_at: Optional[float] = None,
    ) -> None:
        """Reports that an open finding ended through an exception or stoppage,
        e.g. the attempt ended while the issue was ongoing.

        Args:
            finding_id: The open finding.
            summary: Replaces the latest summary if given.
            details: Replaces the latest details if given.
            artifacts: Evidence produced since the last event.
            observed_at: When the observation was made, in seconds since the
                epoch. Defaults to now.
        """
        self._report_open(
            finding_id,
            HealthFinding.State.STOPPED,
            summary=summary,
            details=details,
            artifacts=artifacts,
            observed_at=observed_at,
        )

    def stop_all_open_findings(
        self,
        checker_name: Optional[str] = None,
        observed_at: Optional[float] = None,
    ) -> None:
        """STOPs every open finding, e.g. when the attempt ends.

        Args:
            checker_name: Only stop this checker's findings. Defaults to all.
            observed_at: When the stoppage was observed. Defaults to now.
        """
        for finding_id in self.open_finding_ids(checker_name):
            self.emit_stopped_finding(finding_id, observed_at=observed_at)

    def emit_checker_disabled(
        self,
        checker_name: str,
        reason: int,
        detail: str = "",
        observed_at: Optional[float] = None,
    ) -> None:
        """Reports that a checker gave up for good.

        Its open findings are STOPPED first, and later events from it are
        dropped.

        Args:
            checker_name: The checker that gave up.
            reason: A HealthCheckerDisabled.Reason value.
            detail: Human-readable detail, e.g. the exception message.
            observed_at: When the checker gave up. Defaults to now.
        """
        if checker_name in self._disabled_checkers:
            return
        self.stop_all_open_findings(checker_name, observed_at)
        self._disabled_checkers.add(checker_name)
        self._emit(
            checker_name,
            observed_at,
            checker_disabled=HealthCheckerDisabled(reason=reason, detail=detail),
        )

    def emit_decision(
        self,
        action: str,
        *,
        reason: str = "",
        evaluators: Sequence[str] = (),
        finding_ids: Sequence[str] = (),
        target_node_ids: Sequence[str] = (),
        target_ranks: Sequence[int] = (),
        on_demand_probes: Sequence[str] = (),
        observed_at: Optional[float] = None,
    ) -> Optional[str]:
        """Reports the action the HealthManager is taking.

        Args:
            action: The `Action` enum's name, e.g. "EVICT". "NOOP" is ignored.
            reason: Each contributing evaluator's reason, joined.
            evaluators: The evaluators whose decisions were merged.
            finding_ids: The findings the decision acts on.
            target_node_ids: Hex node IDs, for EVICT and DIAGNOSE.
            target_ranks: World ranks, for DIAGNOSE.
            on_demand_probes: Names of the on-demand probes to run, for
                DIAGNOSE.
            observed_at: When the decision was made. Defaults to now.

        Returns:
            The generated decision ID, or None for NOOP.
        """
        if action == "NOOP":
            return None
        decision = HealthDecision(
            decision_id=uuid.uuid4().hex,
            action=HealthDecision.Action.Value(action),
            reason=reason,
            evaluators=evaluators,
            finding_ids=finding_ids,
            target_node_ids=[bytes.fromhex(n) for n in target_node_ids],
            target_ranks=target_ranks,
            on_demand_probes=on_demand_probes,
        )
        self._emit(HEALTH_MANAGER_CHECKER_NAME, observed_at, decision=decision)
        return decision.decision_id

    def shutdown(self) -> None:
        """Flushes buffered events. Call when the controller shuts down."""
        if not self._enabled:
            return
        from ray._raylet import EventRecorder

        EventRecorder.shutdown()

    def _report_open(self, finding_id: str, state: int, **fields) -> None:
        if finding_id not in self._open_findings:
            logger.warning(
                f"Dropping {HealthFinding.State.Name(state)} event for unknown or "
                f"closed finding {finding_id}."
            )
            return
        self._report(*self._open_findings[finding_id], state, **fields)

    def _report(
        self,
        checker_name: str,
        latest: HealthFinding,
        state: int,
        summary: Optional[str] = None,
        details: Optional[FindingDetails] = None,
        artifacts: Sequence[HealthFinding.HealthArtifact] = (),
        observed_at: Optional[float] = None,
    ) -> None:
        """Emits the finding's next snapshot: `latest` with the given fields
        replaced and only the given artifacts."""
        finding = HealthFinding()
        finding.CopyFrom(latest)
        finding.state = state
        if summary is not None:
            finding.summary = summary
        if isinstance(details, NcclHangFinding):
            finding.nccl_hang.CopyFrom(details)
        elif details is not None:
            finding.ClearField("custom")
            finding.custom.update(details)
        del finding.artifacts[:]
        for artifact in artifacts:
            added = finding.artifacts.add()
            added.CopyFrom(artifact)
            if added.WhichOneof("content") == "text":
                # Drop any multi-byte character the byte cap cuts in half.
                added.text = added.text.encode("utf-8")[
                    :MAX_ARTIFACT_TEXT_BYTES
                ].decode("utf-8", errors="ignore")

        if state in (HealthFinding.State.RESOLVED, HealthFinding.State.STOPPED):
            self._open_findings.pop(finding.finding_id, None)
        else:
            self._open_findings[finding.finding_id] = (checker_name, finding)
        self._emit(checker_name, observed_at, finding=finding)

    def _emit(self, checker_name: str, observed_at: Optional[float], **body) -> None:
        self._sequences[checker_name] += 1
        if not self._enabled:
            return
        event = HealthEvent(
            checker_name=checker_name, sequence=self._sequences[checker_name], **body
        )
        event.observed_at.FromNanoseconds(
            int((observed_at if observed_at is not None else time.time()) * 1e9)
        )
        try:
            self._emit_to_recorder(event)
        except Exception:
            logger.warning(
                f"Failed to export health event from '{checker_name}'.", exc_info=True
            )

    def _emit_to_recorder(self, event: HealthEvent) -> None:
        import ray
        from ray._common.observability.health_events import HealthEventBuilder
        from ray._raylet import EventRecorder

        if not ray.is_initialized():
            logger.warning("Ray is not initialized; health events will not be emitted.")
            return
        node = ray._private.worker.global_worker.node
        if EventRecorder.instance() is None:
            # The aggregator agent is served on the dashboard agent's gRPC
            # server, which listens on metrics_agent_port.
            EventRecorder.initialize(
                aggregator_port=node.metrics_agent_port,
                node_ip=node.node_ip_address,
                node_id_hex=node.node_id,
                max_buffer_size=10000,
                metric_source="health",
            )
        EventRecorder.emit(
            HealthEventBuilder(event, session_name=node.session_name).build()
        )
