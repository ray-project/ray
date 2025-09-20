from typing import TYPE_CHECKING, Callable, Dict, Optional

if TYPE_CHECKING:
    from ray.train import Checkpoint
    from ray.train.v2._internal.execution.context import _TrainingResult


class _ValidationSpec:
    """A specification for validation."""

    def __init__(
        self,
        validate_fn: Optional[Callable[["Checkpoint", Optional[Dict]], Dict]] = None,
        validate_config: Optional[Dict] = None,
    ):
        self.validate_fn = validate_fn
        self.validate_config = validate_config

    def __repr__(self) -> str:
        return f"ValidationSpec(validate_fn={self.validate_fn}, validate_config={self.validate_config})"


class _TrainingReport:
    """A _TrainingResult reported by the user and a _ValidationSpec that describes how to validate it."""

    def __init__(
        self,
        training_result: "_TrainingResult",
        validation_spec: Optional[_ValidationSpec],
    ):
        self.training_result = training_result
        self.validation_spec = validation_spec

    def __repr__(self) -> str:
        return f"TrainingReport(training_result={self.training_result}, validation_spec={self.validation_spec})"
