from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train import Checkpoint


class ReportedCheckpointStatus(Enum):
    """Reported Checkpoint status.

    * COMMITTED: Saved without validation function
    * PENDING_VALIDATION: Saved and pending validation function to finish
    * VALIDATED: Saved and validation function is completed
    """

    COMMITTED = "COMMITTED"
    PENDING_VALIDATION = "PENDING_VALIDATION"
    VALIDATED = "VALIDATED"


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
