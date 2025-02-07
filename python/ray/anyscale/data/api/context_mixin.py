from dataclasses import dataclass
from typing import Optional

from ray.anyscale.data.checkpoint.interfaces import CheckpointConfig


@dataclass
class DataContextMixin:
    """A mix-in class that allows adding Anyscale proprietary
    attributes and methods to :class:`~ray.data.DataContext`."""

    # Configuration for Ray Data checkpointing.
    # If None, checkpointing is disabled.
    checkpoint_config: Optional["CheckpointConfig"] = None
