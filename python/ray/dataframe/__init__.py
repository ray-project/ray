from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
from pandas import (eval, Panel, date_range, MultiIndex)
import threading
import psutil

pd_version = pd.__version__
pd_major = int(pd_version.split(".")[0])
pd_minor = int(pd_version.split(".")[1])

if pd_major == 0 and pd_minor < 22:
    raise Exception("In order to use Pandas on Ray, please upgrade your Pandas"
                    " version to >= 0.22.")

DEFAULT_NPARTITIONS = 4
DEFAULT_ROW_PARTITIONS = 4
DEFAULT_COL_PARTITIONS = 4


def set_npartition_default(n):
    global DEFAULT_NPARTITIONS
    DEFAULT_NPARTITIONS = n


def get_npartitions():
    return DEFAULT_NPARTITIONS

def get_nrowpartitions():
    return DEFAULT_ROW_PARTITIONS

def get_ncolpartitions():
    return DEFAULT_COL_PARTITIONS

def set_nrowpartitions(n):
    global DEFAULT_ROW_PARTITIONS
    DEFAULT_ROW_PARTITIONS = n

def set_ncolpartitions(n):
    global DEFAULT_COL_PARTITIONS
    DEFAULT_COL_PARTITIONS = n

def get_nworkers():
    return NWORKERS

# We import these file after above two function
# because they depend on npartitions.
from .dataframe import DataFrame  # noqa: 402
from .series import Series  # noqa: 402
from .io import (read_csv, read_parquet, read_json, read_html,  # noqa: 402
                 read_clipboard, read_excel, read_hdf, read_feather,  # noqa: 402
                 read_msgpack, read_stata, read_sas, read_pickle,  # noqa: 402
                 read_sql)  # noqa: 402
from .concat import concat  # noqa: 402

__all__ = [
    "DataFrame", "Series", "read_csv", "read_parquet", "concat", "eval",
    "Panel", "date_range", "MultiIndex", "set_nrowpartitions", "set_ncolpartitions"
]

try:
    if threading.current_thread().name == "MainThread":
        import ray
        ray.init(num_cpus=8)
except AssertionError:
    pass

NWORKERS = 8 # psutil.cpu_count()
