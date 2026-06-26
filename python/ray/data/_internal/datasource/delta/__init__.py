"""Delta Lake write datasink for Ray Data.

This module owns the *write* side. The *read* side (``ray.data.read_delta``)
is implemented in ``ray.data.read_api`` and lives outside this package.
"""

from ray.data._internal.datasource.delta.adapter import DeltaAdapter
from ray.data._internal.datasource.delta.datasink import DeltaDatasink

__all__ = [
    "DeltaAdapter",
    "DeltaDatasink",
]
