from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Type

from ray.util.annotations import PublicAPI


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


@dataclass
@PublicAPI(stability="alpha")
class ValidateTaskConfig:
    """Configuration for the validation task.

    This configuration is used when running validation functions asynchronously
    via :meth:`ray.train.report` with a ``validate_fn``.

    Args:
        max_retries: The maximum number of times to retry validation if it fails.
            Defaults to 3.
        retry_exceptions: A list of exception types to retry validation on.
            By default, only Ray errors are retried, not user exceptions.
            If you want to retry on specific user exceptions, provide them here.
    """

    max_retries: int = 3
    retry_exceptions: Optional[List[Type[Exception]]] = None
