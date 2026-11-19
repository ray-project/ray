from typing import TYPE_CHECKING, Dict, Union

from ray._common.deprecation import (
    DeprecatedAlias as _DeprecatedAlias,
    make_deprecated_getattr as _make_deprecated_getattr,
)

if TYPE_CHECKING:
    import numpy
    import pandas  # noqa: F401
    import pyarrow

# The type that `ray.air.DataBatchType` and `ray.air.data_batch_type.DataBatchType`
# resolve to until Ray 3.0 removes them. It stays the original type, so code still
# using the deprecated names behaves as before. New code uses
# `ray.data.block.DataBatch`.
_DataBatchType = Union[
    "numpy.ndarray", "pyarrow.Table", "pandas.DataFrame", Dict[str, "numpy.ndarray"]
]

_DATA_BATCH_TYPE_ALIAS = _DeprecatedAlias(
    "ray.air.data_batch_type:_DataBatchType",
    (("ray.data.block.DataBatch", ""),),
    note=(
        "`DataBatch` has no bare `numpy.ndarray` member; use a dict of arrays. "
        "See this issue for more context and migration options: "
        "https://github.com/ray-project/ray/issues/49454."
    ),
)

# Kept out of the module globals so that `__getattr__` can warn on access.
_DEPRECATED_ALIASES = {"DataBatchType": _DATA_BATCH_TYPE_ALIAS}

__getattr__ = _make_deprecated_getattr(__name__, _DEPRECATED_ALIASES, "3.0")


def __dir__():
    return sorted(set(globals()) | set(_DEPRECATED_ALIASES))
