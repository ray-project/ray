from typing import TYPE_CHECKING

from .interfaces import CheckpointBackend, CheckpointConfig

if TYPE_CHECKING:
    # Static-only imports so type checkers see the lazily-exported names
    # below; at runtime they are resolved by ``__getattr__`` instead, to
    # avoid a circular import: checkpoint_filter -> ray.data.context ->
    # this package.
    from .checkpoint_filter import (  # noqa: F401
        CheckpointFilter,
        CheckpointManager,
        GeneratedIdColumnCheckpointManager,
        IdColumnCheckpointManager,
        NumpyArrayBasedCheckpointFilter,
    )

__all__ = [
    "CheckpointConfig",
    "CheckpointBackend",
    "CheckpointFilter",
    "CheckpointManager",
    "GeneratedIdColumnCheckpointManager",
    "IdColumnCheckpointManager",
    "NumpyArrayBasedCheckpointFilter",
]

_LAZY_EXPORTS = (
    "CheckpointFilter",
    "CheckpointManager",
    "GeneratedIdColumnCheckpointManager",
    "IdColumnCheckpointManager",
    "NumpyArrayBasedCheckpointFilter",
)


def __getattr__(name: str) -> type:
    # See the TYPE_CHECKING block above for why these are lazy.
    if name in _LAZY_EXPORTS:
        from . import checkpoint_filter

        return getattr(checkpoint_filter, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
