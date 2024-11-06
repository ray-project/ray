from dataclasses import dataclass, field

from ray.anyscale.data.checkpoint.interfaces import CheckpointConfig


def _checkpoint_config_factory() -> "CheckpointConfig":
    # Lazily import to avoid circular dependencies.
    from ray.anyscale.data.checkpoint.interfaces import CheckpointConfig

    return CheckpointConfig()


@dataclass
class DataContextMixin:
    """A mix-in class that allows adding Anyscale proprietary
    attributes and methods to :class:`~ray.data.DataContext`."""

    # Configuration for Ray Data checkpointing / resumability.
    checkpoint_config: "CheckpointConfig" = field(
        default_factory=_checkpoint_config_factory
    )

    # Internal boolean, set to True if there are any operators which
    # indicate we should temporarily skip checkpoint filter/writes because
    # they require iterating over the full dataset, including:
    # `Dataset.schema()`, `Dataset.count()`, `Dataset.size_bytes()`.
    _skip_checkpoint_temp: bool = False
