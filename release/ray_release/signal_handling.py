import signal
import sys
from typing import Any, Callable, List, Union

from ray_release.logger import logger

_handling_setup = False
_handler_functions: List[Callable[[int, Any], None]] = []
_signals_to_handle = {
    sig
    for sig in (
        signal.SIGTERM,
        signal.SIGINT,
        signal.SIGQUIT,
        signal.SIGABRT,
    )
    if hasattr(signal, sig.name)
}
_original_handlers = {sig: signal.getsignal(sig) for sig in _signals_to_handle}


def _terminate_handler(signum=None, frame=None):
    logger.info(f"Caught signal {signal.Signals(signum)}, using custom handling...")
    for fn in _handler_functions:
        fn(signum, frame)
    if signum is not None:
        logger.info(f"Exiting with return code {signum}.")
        sys.exit(signum)


def register_handler(fn: Callable[[int, Any], None]):
    """Register a function to be used as a signal handler.

    The function will be placed on top of the stack."""
    assert _handling_setup
    _handler_functions.insert(0, fn)


def unregister_handler(fn_or_index: Union[int, Callable[[int, Any], None]]):
    """Unregister a function by reference or index."""
    assert _handling_setup
    if isinstance(fn_or_index, int):
        _handler_functions.pop(fn_or_index)
    else:
        _handler_functions.remove(fn_or_index)


def setup_signal_handling():
    """Setup custom signal handling.

    Will run all functions registered with ``register_handler`` in order from
    the most recently registered one.
    """
    global _handling_setup
    if not _handling_setup:
        for sig in _signals_to_handle:
            signal.signal(sig, _terminate_handler)
        _handling_setup = True


def reset_signal_handling():
    """Reset custom signal handling back to default handlers."""
    global _handling_setup
    if _handling_setup:
        for sig, handler in _original_handlers.items():
            signal.signal(sig, handler)
        _handling_setup = False
