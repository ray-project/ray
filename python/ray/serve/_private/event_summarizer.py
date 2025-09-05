import json
import logging
import time
from typing import Any, List, Optional, Sequence

from ray.autoscaler._private.event_summarizer import EventSummarizer
from ray.serve._private.common import (
    DecisionSummary,
    DeploymentSnapshot,
    SnapshotSignature,
)
from ray.serve._private.constants import (
    AUTOSCALER_SUMMARIZER_DECISION_LIMIT,
    SERVE_LOGGER_NAME,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ServeAutoscalingEventSummarizer:
    """Serve-specific wrapper around Ray's EventSummarizer.

    Centralizes autoscaling snapshot formatting, decision summarization,
    throttled note emission, and change-signature calculation so controller logic
    remains small and consistent.
    """

    def __init__(self):
        self._summarizer = EventSummarizer()

    def compute_signature(
        self,
        *,
        current_replicas: int,
        proposed_replicas: int,
        min_replicas: Optional[int],
        max_replicas: Optional[int],
        scaling_status: str,
        total_requests: float,
    ) -> SnapshotSignature:
        """Return a hashable signature that represents the visible snapshot state.

        The controller uses this to avoid emitting duplicate logs when nothing
        material changed.
        """
        return (
            int(current_replicas),
            int(proposed_replicas),
            None if min_replicas is None else int(min_replicas),
            None if max_replicas is None else int(max_replicas),
            str(scaling_status),
            float(total_requests or 0.0),
        )

    def summarize_recent_decisions(
        self,
        decisions: Sequence[Any],
        *,
        limit: int = AUTOSCALER_SUMMARIZER_DECISION_LIMIT,
    ) -> List[DecisionSummary]:
        """Convert recent ScalingDecision objects into typed DecisionSummary list for logs."""
        out: List[DecisionSummary] = []
        for d in list(decisions)[-limit:]:
            if hasattr(d, "dict"):
                dd = d.dict()
                ts = dd.get("timestamp_s")
                prev_num_replicas = dd.get("prev_num_replicas")
                curr_num_replicas = dd.get("curr_num_replicas")
                reason = dd.get("reason") or ""
            else:
                ts = getattr(d, "timestamp_s", None)
                prev_num_replicas = getattr(d, "prev_num_replicas", None)
                curr_num_replicas = getattr(d, "curr_num_replicas", None)
                reason = getattr(d, "reason", "") or ""

            timestamp_s = (
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
                if ts is not None
                else None
            )
            if len(reason) > 80:
                reason = reason[:77] + "..."
            out.append(
                DecisionSummary(
                    timestamp_s=timestamp_s,
                    prev_num_replicas=prev_num_replicas,
                    curr_num_replicas=curr_num_replicas,
                    reason=reason,
                )
            )
        return out

    def format_scaling_status(self, scaling_status: str) -> str:
        """Return a human-friendly scaling status string."""
        return {
            "UPSCALING": "scaling up",
            "DOWNSCALING": "scaling down",
            "STABLE": "stable",
        }.get(str(scaling_status), str(scaling_status).lower())

    def format_metrics_health_text(
        self,
        *,
        last_metrics_age_s: Optional[float],
        look_back_period_s: Optional[float],
    ) -> str:
        """Return a short human-friendly health string based on freshness."""
        if last_metrics_age_s is None:
            return "ok"
        threshold = look_back_period_s or 30.0
        if last_metrics_age_s > threshold:
            return f"delayed (last update {int(last_metrics_age_s)}s ago)"
        return "ok"

    def build_deployment_snapshot(
        self,
        *,
        app_name: str,
        deployment_name: str,
        current_replicas: int,
        proposed_replicas: int,
        min_replicas: Optional[int],
        max_replicas: Optional[int],
        scaling_status: str,
        policy_name: str,
        look_back_period_s: Optional[float],
        queued_requests: Optional[float],
        total_requests: float,
        last_metrics_age_s: Optional[float],
        errors: Optional[List[str]],
        recent_decisions: List[DecisionSummary],
    ) -> DeploymentSnapshot:
        """Build the typed snapshot object we log as a one-liner."""
        timestamp_s = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        health_text = self.format_metrics_health_text(
            last_metrics_age_s=last_metrics_age_s, look_back_period_s=look_back_period_s
        )
        return DeploymentSnapshot(
            timestamp_s=timestamp_s,
            app=app_name,
            deployment=deployment_name,
            current_replicas=int(current_replicas),
            target_replicas=int(proposed_replicas),
            min_replicas=None if min_replicas is None else int(min_replicas),
            max_replicas=None if max_replicas is None else int(max_replicas),
            scaling_status=scaling_status,
            policy=policy_name,
            look_back_period_s=look_back_period_s,
            queued_requests=None if queued_requests is None else float(queued_requests),
            total_requests=float(total_requests or 0.0),
            metrics_health=health_text,
            errors=errors or [],
            decisions=recent_decisions,
        )

    def log_snapshot(self, snapshot: DeploymentSnapshot) -> None:
        """Emit the canonical one-line JSON snapshot from typed object."""
        payload = snapshot.to_log_dict()
        logger.info(
            "serve_autoscaling_snapshot " + json.dumps(payload, separators=(",", ":"))
        )

    def emit_deployment_snapshot(
        self,
        *,
        app_name: str,
        deployment_name: str,
        current_replicas: int,
        proposed_replicas: int,
        min_replicas: Optional[int],
        max_replicas: Optional[int],
        scaling_status: str,
        policy_name: str,
        look_back_period_s: Optional[float],
        queued_requests: Optional[float],
        total_requests: float,
        last_metrics_age_s: Optional[float],
        errors: Optional[List[str]],
        recent_decisions: List[DecisionSummary],
    ) -> None:
        """Build and immediately log a deployment snapshot."""
        snapshot = self.build_deployment_snapshot(
            app_name=app_name,
            deployment_name=deployment_name,
            current_replicas=current_replicas,
            proposed_replicas=proposed_replicas,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            scaling_status=scaling_status,
            policy_name=policy_name,
            look_back_period_s=look_back_period_s,
            queued_requests=queued_requests,
            total_requests=total_requests,
            last_metrics_age_s=last_metrics_age_s,
            errors=errors,
            recent_decisions=recent_decisions,
        )
        self.log_snapshot(snapshot)
