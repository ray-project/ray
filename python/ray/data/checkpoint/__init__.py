from .interfaces import CheckpointBackend, CheckpointConfig

__all__ = [
    "CheckpointConfig",
    "CheckpointBackend",
    "CheckpointFilter",
    "NumpyArrayBasedCheckpointFilter",
]


def __getattr__(name):
    # Lazily import filter classes to avoid a circular import:
    # checkpoint_filter -> ray.data.context -> this package.
    if name in ("CheckpointFilter", "NumpyArrayBasedCheckpointFilter"):
        from ray.data.checkpoint import checkpoint_filter

        return getattr(checkpoint_filter, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
