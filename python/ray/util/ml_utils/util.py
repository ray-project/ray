import socket
from contextlib import closing

import numpy as np

from ray.util.annotations import Deprecated


@Deprecated
def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@Deprecated
def is_nan(value):
    return np.isnan(value)


@Deprecated
def is_nan_or_inf(value):
    return is_nan(value) or np.isinf(value)
