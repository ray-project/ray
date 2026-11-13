from typing import Tuple as _Tuple

from ray.air import config, result  # noqa: F401
from ray.air._internal.deprecation import (
    DeprecatedAlias as _DeprecatedAlias,
    make_deprecated_getattr as _make_deprecated_getattr,
)
from ray.air.data_batch_type import DataBatchType
from ray.air.execution.resources.request import AcquiredResources, ResourceRequest
import ray.data  # noqa: F401  # TODO: This is a hack to avoid circular import


def _tune_or_train(name: str) -> _Tuple[_Tuple[str, str], ...]:
    return (
        (f"ray.tune.{name}", "with Ray Tune"),
        (f"ray.train.{name}", "with Ray Train"),
    )


# Deprecated in favour of the `ray.tune` / `ray.train` paths; removed in Ray 3.0.
# These are kept out of the module globals so that `__getattr__` can warn on access.
_DEPRECATED_ALIASES = {
    "RunConfig": _DeprecatedAlias(
        "ray.air.config:RunConfig", _tune_or_train("RunConfig")
    ),
    "CheckpointConfig": _DeprecatedAlias(
        "ray.air.config:CheckpointConfig", _tune_or_train("CheckpointConfig")
    ),
    "FailureConfig": _DeprecatedAlias(
        "ray.air.config:FailureConfig", _tune_or_train("FailureConfig")
    ),
    "ScalingConfig": _DeprecatedAlias(
        "ray.air.config:ScalingConfig", (("ray.train.ScalingConfig", ""),)
    ),
    "Result": _DeprecatedAlias("ray.air.result:Result", _tune_or_train("Result")),
}

__getattr__ = _make_deprecated_getattr(__name__, _DEPRECATED_ALIASES)


def __dir__():
    return sorted(set(globals()) | set(_DEPRECATED_ALIASES))


__all__ = [
    "DataBatchType",
    "RunConfig",
    "Result",
    "ScalingConfig",
    "FailureConfig",
    "CheckpointConfig",
    "AcquiredResources",
    "ResourceRequest",
]
