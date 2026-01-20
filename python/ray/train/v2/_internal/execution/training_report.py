from typing import TYPE_CHECKING, Any, Dict, Optional, Union

if TYPE_CHECKING:
    from ray.train import Checkpoint
    from ray.train.v2.api.report_config import ValidationTaskConfig


class _TrainingReport:
    """Checkpoint and metrics reported by user, as well as optional validation configuration."""

    def __init__(
        self,
        checkpoint: Optional["Checkpoint"],
        metrics: Dict[str, Any],
        validate: Union[bool, "ValidationTaskConfig"],
    ):
        self.checkpoint = checkpoint
        self.metrics = metrics
        self.validate = validate

    def __repr__(self) -> str:
        return f"TrainingReport(checkpoint={self.checkpoint}, metrics={self.metrics}, validate={self.validate})"
