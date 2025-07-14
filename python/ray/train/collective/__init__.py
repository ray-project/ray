from ray.train.v2._internal.constants import is_v2_enabled

if is_v2_enabled():
    from ray.train.collective.collectives import barrier, broadcast_from_rank_zero

    __all__ = [
        "broadcast_from_rank_zero",
        "barrier",
    ]

    broadcast_from_rank_zero.__module__ = "ray.train.collective"
    barrier.__module__ = "ray.train.collective"
else:
    raise ImportError(
        "`ray.train.collective` is only available in Ray Train v2. "
        "To enable it, please set `RAY_TRAIN_V2_ENABLED=1`."
    )

# DO NOT ADD ANYTHING AFTER THIS LINE.
