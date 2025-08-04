from dataclasses import dataclass
from typing import Any, Dict

from ray.train import Checkpoint


@dataclass
class TrainingResult:
    """A user-reported checkpoint and its associated metrics.

    Attributes:
        checkpoint: The checkpoint reported by the user.
        metrics: The metrics associated with that checkpoint.
    """

    checkpoint: Checkpoint
    metrics: Dict[str, Any]
