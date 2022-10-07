_deprecation_msg = (
    "`ray.train.callbacks` and the `ray.train.Trainer` API are deprecated in Ray "
    "2.0, and are replaced by Ray AI Runtime (Ray AIR). Ray AIR "
    "(https://docs.ray.io/en/latest/ray-air/getting-started.html) "
    "provides greater functionality and a unified API "
    "compared to the current Ray Train API. "
    "This module will be removed in the future."
)

raise DeprecationWarning(_deprecation_msg)
