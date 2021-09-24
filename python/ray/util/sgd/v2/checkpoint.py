from dataclasses import dataclass
from typing import Optional

from ray.util.sgd.v2.constants import TIMESTAMP

MAX = "max"
MIN = "min"


@dataclass
class CheckpointStrategy:
    """Configurable parameters for defining the SGD checkpointing strategy.

    Default behavior is to persist all checkpoints to disk. If
    ``num_to_keep`` is set, the default retention policy is to keep the
    checkpoints with maximum timestamp, i.e. the most recent checkpoints.

    Args:
        num_to_keep (Optional[int]): The number of checkpoints to keep
            on disk for this run. If a checkpoint is persisted to disk after
            there are already this many checkpoints, then an existing
            checkpoint will be deleted. If this is ``None`` then checkpoints
            will not be deleted. If this is ``0`` then no checkpoints will be
            persisted to disk.
        checkpoint_score_attribute (str): The attribute that will be used to
            score checkpoints to determine which checkpoints should be kept
            on disk when there are greater than ``num_to_keep`` checkpoints.
            This attribute must be a key from the checkpoint
            dictionary which has a numerical value.
        checkpoint_score_order (str). Either "max" or "min".
            If "max", then checkpoints with highest values of
            ``checkpoint_score_attribute`` will be kept.
            If "min", then checkpoints with lowest values of
            ``checkpoint_score_attribute`` will be kept.
    """
    num_to_keep: Optional[int] = None
    checkpoint_score_attribute: str = TIMESTAMP
    checkpoint_score_order: str = MAX

    def __post_init__(self):
        if self.num_to_keep:
            # TODO(matt): Implement num_to_keep deletion.
            raise NotImplementedError("Deleting checkpoints is not yet "
                                      "supported. Please use None to persist "
                                      "all checkpoints or 0 to not persist "
                                      "any checkpoints.")
        if self.checkpoint_score_order not in (MAX, MIN):
            raise ValueError(f"checkpoint_score_order must be either "
                             f"\"{MAX}\" or \"{MIN}\".")
