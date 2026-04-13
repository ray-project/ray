import warnings

warnings.warn(
    "Importing SaveMode from ray.data._internal.utils.savemode is deprecated. "
    "Use ray.data._internal.public_api.savemode instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ray.data._internal.public_api.savemode import SaveMode  # noqa: E402, F401

__all__ = ["SaveMode"]
