from typing import Tuple as _Tuple

from ray._common.deprecation import (
    DeprecatedAlias as _DeprecatedAlias,
    make_deprecated_getattr as _make_deprecated_getattr,
)
from ray.air import config, result  # noqa: F401
from ray.air.data_batch_type import _DATA_BATCH_TYPE_ALIAS
from ray.air.execution.resources.request import AcquiredResources, ResourceRequest
import ray.data  # noqa: F401  # TODO: This is a hack to avoid circular import


_REMOVAL_VERSION = "3.0"
_MIGRATION_NOTE = (
    "See this issue for more context and migration options: "
    "https://github.com/ray-project/ray/issues/49454."
)


def _alias(target: str, *replacements: _Tuple[str, str]) -> _DeprecatedAlias:
    return _DeprecatedAlias(target, replacements, note=_MIGRATION_NOTE)


def _tune_or_train(name: str) -> _Tuple[_Tuple[str, str], ...]:
    return (
        (f"ray.tune.{name}", "with Ray Tune"),
        (f"ray.train.{name}", "with Ray Train"),
    )


# Deprecated in favour of the `ray.tune` / `ray.train` paths; removed in Ray 3.0.
# These are kept out of the module globals so that `__getattr__` can warn on access.
_DEPRECATED_ALIASES = {
    "RunConfig": _alias("ray.air.config:RunConfig", *_tune_or_train("RunConfig")),
    "CheckpointConfig": _alias(
        "ray.air.config:CheckpointConfig", *_tune_or_train("CheckpointConfig")
    ),
    "FailureConfig": _alias(
        "ray.air.config:FailureConfig", *_tune_or_train("FailureConfig")
    ),
    "ScalingConfig": _alias(
        "ray.air.config:ScalingConfig", ("ray.train.ScalingConfig", "")
    ),
    "Result": _alias("ray.air.result:Result", *_tune_or_train("Result")),
    # Deprecated in favour of `ray.data.block.DataBatch`.
    "DataBatchType": _DATA_BATCH_TYPE_ALIAS,
}

__getattr__ = _make_deprecated_getattr(__name__, _DEPRECATED_ALIASES, _REMOVAL_VERSION)


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
