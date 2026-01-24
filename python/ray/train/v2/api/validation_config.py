from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train import Checkpoint


@PublicAPI(stability="alpha")
class ValidationFn(Protocol):
    """Protocol for a function that validates a checkpoint."""

    def __call__(self, checkpoint: "Checkpoint", **kwargs: Any) -> Dict:
        ...


@dataclass
@PublicAPI(stability="alpha")
class ValidationTaskConfig:
    """Configuration for a specific validation task, passed to report().

    Args:
        fn_kwargs: json-serializable keyword arguments to pass to the validation function.
            Note that we always pass `checkpoint` as the first argument to the
            validation function.
        ray_remote_kwargs: Keyword arguments to pass to `ray.remote()` for the validation task.
            This can be used to specify resource requirements, number of retries, etc.
    """

    fn_kwargs: Optional[Dict[str, Any]] = None
    ray_remote_kwargs: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.fn_kwargs is None:
            self.fn_kwargs = {}
        if self.ray_remote_kwargs is None:
            self.ray_remote_kwargs = {}


@PublicAPI(stability="alpha")
class ValidationConfig:
    """Configuration for validation, passed to the trainer.

    Args:
        fn: The validation function to run on checkpoints.
            This function should accept a checkpoint as the first argument
            and return a dictionary of metrics.
        task_config: Default configuration for validation tasks.
            The fn_kwargs in this config can be overridden by
            ValidationTaskConfig passed to report().
    """

    def __init__(
        self,
        fn: ValidationFn,
        task_config: Optional[ValidationTaskConfig] = None,
    ):
        self.fn = fn
        if task_config is None:
            self.task_config = ValidationTaskConfig()
        else:
            self.task_config = task_config
