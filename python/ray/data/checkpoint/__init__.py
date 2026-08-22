from .interfaces import CheckpointBackend, CheckpointConfig

__all__ = [
    "CheckpointConfig",
    "CheckpointBackend",
    "CheckpointFilter",
    "CheckpointManager",
    "IdColumnCheckpointManager",
    "NumpyArrayBasedCheckpointFilter",
]

_LAZY_EXPORTS = (
    "CheckpointFilter",
    "CheckpointManager",
    "IdColumnCheckpointManager",
    "NumpyArrayBasedCheckpointFilter",
)


def __getattr__(name: str) -> type:
    # Lazily import filter/manager classes to avoid a circular import:
    # checkpoint_filter -> ray.data.context -> this package.
    if name in _LAZY_EXPORTS:
        from ray.data.checkpoint import checkpoint_filter

        return getattr(checkpoint_filter, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
