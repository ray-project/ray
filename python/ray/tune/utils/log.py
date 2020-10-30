from enum import Enum
from typing import Any, Callable, Union


class Verbosity(Enum):
    MINIMAL = 0
    EXPERIMENT = 1
    TRIAL_NORM = 2
    TRIAL_DETAILS = 3

    def __int__(self):
        return self.value


verbosity: Union[int, Verbosity] = Verbosity.TRIAL_DETAILS


def verbose_log(logger: Callable[[str], Any], level: Union[int, Verbosity],
                message: str):
    """Log `message` if specified level exceeds global verbosity level.

    `logger` should be a Callable, e.g. `logger.info`. It can also be
    `print` or a logger method of any other level - or any callable that
    accepts a string.
    """
    if has_verbosity(level):
        logger(message)


def has_verbosity(level: Union[int, Verbosity]) -> bool:
    """Return True if passed level exceeds global verbosity level."""
    global verbosity

    log_level = int(level)
    verbosity_level = int(verbosity)

    return log_level >= verbosity_level
