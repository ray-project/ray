from dataclasses import dataclass
from enum import Enum
import os
from typing import Union

from ray.util import PublicAPI
from ray.util.annotations import DeveloperAPI


#################################
#         OLD FLOW              #
#################################


@PublicAPI
class Verbosity(Enum):
    V0_MINIMAL = 0
    V1_EXPERIMENT = 1
    V2_TRIAL_NORM = 2
    V3_TRIAL_DETAILS = 3

    def __int__(self):
        return self.value


verbosity: Union[int, Verbosity] = Verbosity.V3_TRIAL_DETAILS


@DeveloperAPI
def set_verbosity(level: Union[int, Verbosity]):
    global verbosity

    if verbosity == Verbosity.V0_MINIMAL:
        return

    if "AIR_VERBOSITY" in os.environ:
        # DF traffic - disable the old flow by silencing it.
        verbosity = Verbosity.V0_MINIMAL
        return

    if isinstance(level, int):
        verbosity = Verbosity(level)
    else:
        verbosity = level


@DeveloperAPI
def has_verbosity(level: Union[int, Verbosity]) -> bool:
    """Return True if passed level exceeds global verbosity level."""
    global verbosity

    log_level = int(level)
    verbosity_level = int(verbosity)

    return verbosity_level >= log_level


# TODO: find a file under air instead of tune to host this.
#################################
#         NEW FLOW              #
#################################


#  Note: also need to ship this env var to remote actor.
def has_verbosity_new(level: int) -> bool:
    if "AIR_VERBOSITY" not in os.environ:
        # effectively disabling the new flow
        return False
    return int(os.environ["AIR_VERBOSITY"]) >= level


@dataclass
class _ExecutionType:
    """Execution type for logging purposes."""

    rllib: bool = False
    single_trial: bool = False


# TODO: Ideally this should be explicitly passed around.
#  But due to the current structure of our code (need to check
#  this everywhere) host it as a global for now.
_execution_type = _ExecutionType()


def set_execution_type(is_rllib, is_single_trial):
    global _execution_type
    _execution_type.rllib = is_rllib
    _execution_type.single_trial = is_single_trial


def get_execution_type():
    global _execution_type
    return _execution_type


@DeveloperAPI
def disable_ipython():
    """Disable output of IPython HTML objects."""
    try:
        from IPython.core.interactiveshell import InteractiveShell

        InteractiveShell.clear_instance()
    except Exception:
        pass
