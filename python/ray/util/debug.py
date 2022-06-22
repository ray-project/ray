import time

from ray.util.annotations import DeveloperAPI

_logged = set()
_disabled = False
_periodic_log = False
_last_logged = 0.0


@DeveloperAPI
def log_once(key):
    """Returns True if this is the "first" call for a given key.

    Various logging settings can adjust the definition of "first".

    Example:
        >>> if log_once("some_key"):
        ...     logger.info("Some verbose logging statement")
    """

    global _last_logged

    if _disabled:
        return False
    elif key not in _logged:
        _logged.add(key)
        _last_logged = time.time()
        return True
    elif _periodic_log and time.time() - _last_logged > 60.0:
        _logged.clear()
        _last_logged = time.time()
        return False
    else:
        return False


@DeveloperAPI
def disable_log_once_globally():
    """Make log_once() return False in this process."""

    global _disabled
    _disabled = True


@DeveloperAPI
def enable_periodic_logging():
    """Make log_once() periodically return True in this process."""

    global _periodic_log
    _periodic_log = True


@DeveloperAPI
def reset_log_once(key):
    """Resets log_once for the provided key."""

    _logged.discard(key)
