from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train import Checkpoint


@PublicAPI(stability="alpha")
class CheckpointUploadMode(Enum):
    """The manner in which we want to upload the checkpoint.

    Members:
        ASYNC: Upload checkpoint asynchronously.
        SYNC: Upload checkpoint synchronously.
        NO_UPLOAD: Do not upload checkpoint.
    """

    ASYNC = "ASYNC"
    SYNC = "SYNC"
    NO_UPLOAD = "NO_UPLOAD"

    def _default_delete_local_checkpoint_after_upload(self) -> bool:
        return self == CheckpointUploadMode.ASYNC


@PublicAPI(stability="alpha")
class CheckpointConsistencyMode(Enum):
    """Read semantics for checkpoint retrieval during an ongoing run.

    Members:
        COMMITTED: Block until the checkpoint from the latest ray.train.report
            has been uploaded and committed.
        VALIDATED: Block until the checkpoint from the latest ray.train.report
            has been uploaded and validated.
    """

    COMMITTED = "COMMITTED"
    VALIDATED = "VALIDATED"


@PublicAPI(stability="alpha")
class ValidateFn(Protocol):
    """Protocol for a function that validates a checkpoint."""

    def __call__(self, checkpoint: "Checkpoint", **kwargs: Any) -> Dict:
        ...


@dataclass
@PublicAPI(stability="alpha")
class ValidationTaskConfig:
    """Configuration for a specific validation task, passed to report().

    Args:
        func_kwargs: json-serializable keyword arguments to pass to the validation function.
            Note that we always pass `checkpoint` as the first argument to the
            validation function.
    """

    func_kwargs: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.func_kwargs is None:
            self.func_kwargs = {}


@PublicAPI(stability="alpha")
class ValidationConfig:
    """Configuration for validation, passed to the trainer.

    Args:
        validate_fn: The validation function to run on checkpoints.
            This function should accept a checkpoint as the first argument
            and return a dictionary of metrics.
        validation_task_config: Default configuration for validation tasks.
            The func_kwargs in this config can be overridden by
            ValidationTaskConfig passed to report().
    """

    def __init__(
        self,
        validate_fn: ValidateFn,
        validation_task_config: Optional[ValidationTaskConfig] = None,
    ):
        self.validate_fn = validate_fn
        if validation_task_config is None:
            self.validation_task_config = ValidationTaskConfig()
        else:
            self.validation_task_config = validation_task_config
