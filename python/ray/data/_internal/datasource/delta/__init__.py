"""Delta Lake write support for Ray Data."""

from ray.data._internal.datasource.delta.datasink import DeltaDatasink
from ray.data._internal.datasource.delta.utils import DeltaWriteResult

__all__ = [
    "DeltaDatasink",
    "DeltaWriteResult",
]
