from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train import Checkpoint


@PublicAPI(stability="alpha")
class ReportedCheckpointStatus(Enum):
    """ReportedCheckpoint status enum.

    * COMMITTED: The checkpoint is saved, and no validation was requested.
    * PENDING_VALIDATION: The checkpoint is saved, and validation is in progress.
    * VALIDATED: The checkpoint is saved, and validation is complete.
    * VALIDATED_TIMEOUT: The checkpoint is saved, and validation is timed out according to
        `ValidationTaskConfig(..., timeout_s=N)`.
    * VALIDATION_FAILED: The checkpoint is saved, and validation failed / cancelled.
    """

    COMMITTED = "COMMITTED"
    PENDING_VALIDATION = "PENDING_VALIDATION"
    VALIDATED = "VALIDATED"
    VALIDATION_TIMEOUT = "VALIDATION_TIMEOUT"
    VALIDATION_FAILED = "VALIDATION_FAILED"


@dataclass
@PublicAPI(stability="alpha")
class ReportedCheckpoint:
    """A user-reported checkpoint and its associated metrics.

    Attributes:
        checkpoint: The checkpoint reported by the user.
        metrics: The metrics associated with that checkpoint.
        status: The status of the checkpoint.
    """

    checkpoint: "Checkpoint"
    metrics: Dict[str, Any]
    status: ReportedCheckpointStatus
