from enum import Enum
from typing import Any, Callable, Union


class Verbosity(Enum):
    MINIMAL = 0
    EXPERIMENT = 1
    TRIAL_NORM = 2
    TRIAL_DETAILS = 3


verbosity: Union[int, Verbosity] = Verbosity.TRIAL_DETAILS


def verbose_log(level: Union[int, Verbosity], logger: Callable[[str], Any],
                message: str):
    """Log `message` if specified level exceeds global verbosity level.

    `logger` should be a Callable, e.g. `logger.info`. It can also be
    `print` or a logger method of any other level - or any callable that
    accepts a string.
    """
    global verbosity

    log_level = level.value if isinstance(level, Verbosity) else int(level)
    verbosity_level = verbosity.value if isinstance(
        verbosity, Verbosity) else int(verbosity)

    if log_level >= verbosity_level:
        logger(message)
