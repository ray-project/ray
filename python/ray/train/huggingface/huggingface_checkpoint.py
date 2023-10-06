from ._deprecation_msg import deprecation_msg
from ray.util.annotations import Deprecated


# TODO(ml-team): [code_removal]
@Deprecated(message=deprecation_msg)
class HuggingFaceCheckpoint:
    # Use __new__ as it is much less likely to be overriden
    # than __init__
    def __new__(cls: type, *args, **kwargs):
        raise DeprecationWarning


__all__ = [
    "HuggingFaceCheckpoint",
]
