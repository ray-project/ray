from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

if TYPE_CHECKING:
    from ray.train import Checkpoint


class _ValidationSpec:
    """A specification for validation."""

    def __init__(
        self,
        validate_fn: Optional[Callable[["Checkpoint", Optional[Dict]], Dict]] = None,
        validate_config: Optional[Dict] = None,
        checkpoint: Optional["Checkpoint"] = None,
    ):
        self.validate_fn = validate_fn
        self.validate_config = validate_config
        self.checkpoint = checkpoint

    def __repr__(self) -> str:
        return f"ValidationSpec(validate_fn={self.validate_fn}, validate_config={self.validate_config}, checkpoint={self.checkpoint})"


class _TrainingReport:
    """A _TrainingResult reported by the user and a _ValidationSpec that describes how to validate it."""

    def __init__(
        self,
        checkpoint: Optional["Checkpoint"],
        metrics: Dict[str, Any],
        validation_spec: Optional[_ValidationSpec],
    ):
        self.checkpoint = checkpoint
        self.metrics = metrics
        self.validation_spec = validation_spec

    def __repr__(self) -> str:
        return f"TrainingReport(checkpoint={self.checkpoint}, metrics={self.metrics}, validation_spec={self.validation_spec})"
