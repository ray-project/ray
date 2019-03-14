"""
File adopted from Michael Whittaker.

This module firstly offers a convenience function called "debug" which
alleviates a couple of inconveniences in python logging:

* No need to find a logger before logging (uses the one from this package)
* Slightly friendly string interpolation interface.
"""

from datetime import datetime
import inspect
import hashlib
import subprocess
import os
import shlex
import json
import sys
import logging

class TrackLogHandler(logging.FileHandler):
    """File-based logging handler for the track package"""
    pass

class StdoutHandler(logging.StreamHandler):
    """As described by the name"""
    def __init__(self):
        super().__init__(sys.stdout)

def init(track_log_handler):
    """
    (Re)initialize track's file handler for track package logger.

    Adds a stdout-printing handler automatically.
    """

    logger = logging.getLogger(__package__)

    # TODO (just document prominently)
    # assume only one trial can run at once right now
    # multi-concurrent-trial support will require complex filter logic
    # based on the currently-running trial (maybe we shouldn't allow multiple
    # trials on different python threads, that's dumb)
    to_rm = [h for h in logger.handlers if isinstance(h, TrackLogHandler)]
    for h in to_rm:
        logger.removeHandler(h)

    if not any(isinstance(h, StdoutHandler) for h in logger.handlers):
        handler = StdoutHandler()
        handler.setFormatter(_FORMATTER)
        logger.addHandler(handler)

    track_log_handler.setFormatter(_FORMATTER)
    logger.addHandler(track_log_handler)

    logger.propagate = False
    logger.setLevel(logging.DEBUG)

def debug(s, *args):
    """debug(s, x1, ..., xn) logs s.format(x1, ..., xn)."""
    # Get the path name and line number of the function which called us.
    previous_frame = inspect.currentframe().f_back
    try:
        pathname, lineno, _, _, _ = inspect.getframeinfo(previous_frame)
        # if path is in cwd, simplify it
        cwd = os.path.abspath(os.getcwd())
        pathname = os.path.abspath(pathname)
        if os.path.commonprefix([cwd, pathname]) == cwd:
            pathname = os.path.relpath(pathname, cwd)
    except Exception:  # pylint: disable=broad-except
        pathname = '<UNKNOWN-FILE>.py'
        lineno = 0
    if _FORMATTER: # log could have not been initialized.
        _FORMATTER.pathname = pathname
        _FORMATTER.lineno = lineno
    logger = logging.getLogger(__package__)
    logger.debug(s.format(*args))

class _StackCrawlingFormatter(logging.Formatter):
    """
    If we configure a python logger with the format string
    "%(pathname):%(lineno): %(message)", messages logged via `log.debug` will
    be prefixed with the path name and line number of the code that called
    `log.debug`. Unfortunately, when a `log.debug` call is wrapped in a helper
    function (e.g. debug below), the path name and line number is always that
    of the helper function, not the function which called the helper function.

    A _StackCrawlingFormatter is a hack to log a different pathname and line
    number. Simply set the `pathname` and `lineno` attributes of the formatter
    before you call `log.debug`. See `debug` below for an example.
    """

    def __init__(self, format_str):
        super().__init__(format_str)
        self.pathname = None
        self.lineno = None

    def format(self, record):
        s = super().format(record)
        if self.pathname is not None:
            s = s.replace('{pathname}', self.pathname)
        if self.lineno is not None:
            s = s.replace('{lineno}', str(self.lineno))
        return s

_FORMAT_STRING = "[%(asctime)-15s {pathname}:{lineno}] %(message)s"
_FORMATTER = _StackCrawlingFormatter(_FORMAT_STRING)
