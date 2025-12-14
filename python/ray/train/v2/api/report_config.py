from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Protocol

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
class ValidationConfig:
    """Configuration for validating your reported checkpoint.

    Args:
        func_kwargs: Keyword arguments to pass to the validation function.
            Note that we always pass `checkpoint` as the first argument to the
            validation function.
    """

    func_kwargs: Dict[str, Any]
