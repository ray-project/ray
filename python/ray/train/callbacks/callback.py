import abc
from ray.util.annotations import Deprecated

_deprecation_msg = (
    "`ray.train.callbacks` and the `ray.train.Trainer` API are deprecated in Ray "
    "2.0, and are replaced by Ray AI Runtime (Ray AIR). Ray AIR "
    "(https://docs.ray.io/en/latest/ray-air/getting-started.html) "
    "provides greater functionality and a unified API "
    "compared to the current Ray Train API. "
    "This class will be removed in the future."
)


@Deprecated(message=_deprecation_msg)
class TrainingCallback(abc.ABC):
    """Abstract Train callback class."""

    # Use __new__ as it is much less likely to be overriden
    # than __init__
    def __new__(cls: type):
        raise DeprecationWarning(_deprecation_msg)
