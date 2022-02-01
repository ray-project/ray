from enum import Enum
from typing import Union


class Verbosity(Enum):
    V0_MINIMAL = 0
    V1_EXPERIMENT = 1
    V2_TRIAL_NORM = 2
    V3_TRIAL_DETAILS = 3

    def __int__(self):
        return self.value


verbosity: Union[int, Verbosity] = Verbosity.V3_TRIAL_DETAILS


def set_verbosity(level: Union[int, Verbosity]):
    global verbosity

    if isinstance(level, int):
        verbosity = Verbosity(level)
    else:
        verbosity = level


def has_verbosity(level: Union[int, Verbosity]) -> bool:
    """Return True if passed level exceeds global verbosity level."""
    global verbosity

    log_level = int(level)
    verbosity_level = int(verbosity)

    return verbosity_level >= log_level


def disable_ipython():
    """Disable output of IPython HTML objects."""
    try:
        from IPython.core.interactiveshell import InteractiveShell

        InteractiveShell.clear_instance()
    except Exception:
        pass
