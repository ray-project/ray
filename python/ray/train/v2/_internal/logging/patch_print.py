import builtins
import contextlib
import logging
import sys
from typing import Callable

from ray._private.ray_constants import env_bool
from ray.train.v2._internal.constants import (
    DEFAULT_ENABLE_PRINT_PATCH,
    ENABLE_PRINT_PATCH_ENV_VAR,
)

# Save the original print function
_original_print = builtins.print


@contextlib.contextmanager
def print_context_manager(print_fn: Callable):
    """Context manager to set the builtin print function as print_fn."""
    current_print = builtins.print
    builtins.print = print_fn
    yield
    builtins.print = current_print


def redirected_print(*objects, sep=" ", end="\n", file=None, flush=False):
    """Implement python's print function to redirect logs to Train's logger.

    If the file is set to anything other than stdout, stderr, or None, call the
    builtin print. Else, construct the message and redirect to Train's logger.

    This makes sure that print to customized file in user defined function will not
    be overwritten by the redirected print function.

    See https://docs.python.org/3/library/functions.html#print
    """
    # TODO (hpguo): This handler class is shared by both ray train and ray serve. We
    # should move this to ray core and make it available to both libraries.

    if file not in [sys.stdout, sys.stderr, None]:
        return _original_print(objects, sep=sep, end=end, file=file, flush=flush)

    root_logger = logging.getLogger()
    message = sep.join(map(str, objects))
    # Use the original `print` method for the scope of the logger call, in order to
    # avoid infinite recursion errors if any exceptions get raised (since exception
    # handling involves another `print(..., file=sys.stderr)`.
    # Note that an exception being raised here is not expected (e.g. it would be a
    # bug in our own logging code), so this is just to keep the error logs sane
    # during development.
    with print_context_manager(_original_print):
        # We want this log to be associated with the line of code where user calls
        # `print`, which is stacklevel 2.
        # Frame [stacklevel]:
        # User's call to print [2] -> `redirected_print` [1] -> root_logger.log [0]
        root_logger.log(logging.INFO, message, stacklevel=2)


def patch_print_function() -> None:
    """
    Patch the print function to redirect logs to Train's logger.
    Only patch the print function if the environment variable is set to "1"
    """
    if env_bool(ENABLE_PRINT_PATCH_ENV_VAR, DEFAULT_ENABLE_PRINT_PATCH):
        builtins.print = redirected_print
