from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from ray.train import Checkpoint
    from ray.train.v2.api.report_config import ValidationConfig


class _TrainingReport:
    """Checkpoint and metrics reported by user, as well as optional validation configuration."""

    def __init__(
        self,
        checkpoint: Optional["Checkpoint"],
        metrics: Dict[str, Any],
        validation: Optional["ValidationConfig"],
    ):
        self.checkpoint = checkpoint
        self.metrics = metrics
        self.validation = validation

    def __repr__(self) -> str:
        return f"TrainingReport(checkpoint={self.checkpoint}, metrics={self.metrics}, validation={self.validation})"
