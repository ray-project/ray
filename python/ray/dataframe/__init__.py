from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import threading

DEFAULT_NPARTITIONS = 4


def set_npartition_default(n):
    global DEFAULT_NPARTITIONS
    DEFAULT_NPARTITIONS = n


def get_npartitions():
    return DEFAULT_NPARTITIONS


# We import these file after above two function
# because they depend on npartitions.
from .dataframe import DataFrame  # noqa: 402
from .dataframe import from_pandas  # noqa: 402
from .dataframe import to_pandas  # noqa: 402
from .series import Series  # noqa: 402
from .io import (read_csv, read_parquet)  # noqa: 402

__all__ = [
    "DataFrame", "from_pandas", "to_pandas", "Series", "read_csv",
    "read_parquet"
]

try:
    if threading.current_thread().name == "MainThread":
        import ray
        ray.init()
except AssertionError:
    pass
