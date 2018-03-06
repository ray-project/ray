from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import threading

pd_version = pd.__version__
pd_major = int(pd_version.split(".")[0])
pd_minor = int(pd_version.split(".")[1])

if pd_major == 0 and pd_minor < 22:
    raise Exception("In order to use Pandas on Ray, please upgrade your Pandas"
                    " version to >= 0.22.")

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
