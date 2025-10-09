from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train import Checkpoint


@dataclass
@PublicAPI(stability="alpha")
class ReportedCheckpoint:
    """A user-reported checkpoint and its associated metrics.

    Attributes:
        checkpoint: The checkpoint reported by the user.
        metrics: The metrics associated with that checkpoint.
    """

    checkpoint: "Checkpoint"
    metrics: Dict[str, Any]
