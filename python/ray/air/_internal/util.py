import socket
from contextlib import closing

import numpy as np


def find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def is_nan(value):
    return np.isnan(value)


def is_nan_or_inf(value):
    return is_nan(value) or np.isinf(value)


def shorten_tb(tb, attr: str):
    orig_tb = tb
    while tb:
        print("CURRENT GLOBALS", tb.tb_frame.f_locals.keys(), "??", attr)

        if tb.tb_frame.f_locals.get(attr):
            return tb
        tb = tb.tb_next

    return orig_tb
