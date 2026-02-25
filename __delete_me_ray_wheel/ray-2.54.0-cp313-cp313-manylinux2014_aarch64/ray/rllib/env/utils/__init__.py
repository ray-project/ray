import logging
from typing import Type, Union

import gymnasium as gym

from ray.rllib.env.env_context import EnvContext
from ray.rllib.utils.error import (
    ERR_MSG_INVALID_ENV_DESCRIPTOR,
    EnvError,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
def try_import_pyspiel(error: bool = False):
    """Tries importing pyspiel and returns the module (or None).

    Args:
        error: Whether to raise an error if pyspiel cannot be imported.

    Returns:
        The pyspiel module.

    Raises:
        ImportError: If error=True and pyspiel is not installed.
    """
    try:
        import pyspiel

        return pyspiel
    except ImportError:
        if error:
            raise ImportError(
                "Could not import pyspiel! Pyspiel is not a dependency of RLlib "
                "and RLlib requires you to install pyspiel separately: "
                "`pip install open_spiel`."
            )
        return None


@PublicAPI
def try_import_open_spiel(error: bool = False):
    """Tries importing open_spiel and returns the module (or None).

    Args:
        error: Whether to raise an error if open_spiel cannot be imported.

    Returns:
        The open_spiel module.

    Raises:
        ImportError: If error=True and open_spiel is not installed.
    """
    try:
        import open_spiel

        return open_spiel
    except ImportError:
        if error:
            raise ImportError(
                "Could not import open_spiel! open_spiel is not a dependency of RLlib "
                "and RLlib requires you to install open_spiel separately: "
                "`pip install open_spiel`."
            )
        return None


def _gym_env_creator(
    env_context: EnvContext,
    env_descriptor: Union[str, Type[gym.Env]],
) -> gym.Env:
    """Tries to create a gym env given an EnvContext object and descriptor.

    Note: This function tries to construct the env from a string descriptor
    only using possibly installed RL env packages (such as gymnasium).
    These packages are no installation requirements for RLlib. In case
    you would like to support more such env packages, add the necessary imports
    and construction logic below.

    Args:
        env_context: The env context object to configure the env.
            Note that this is a config dict, plus the properties:
            `worker_index`, `vector_index`, and `remote`.
        env_descriptor: The env descriptor as a gym-registered string, e.g.
            "CartPole-v1", "ale_py:ALE/Breakout-v5".
            Alternatively, the gym.Env subclass to use.

    Returns:
        The actual gym environment object.

    Raises:
        gym.error.Error: If the env cannot be constructed.
    """
    # If env descriptor is a str, starting with "ale_py:ALE/", for now, register all ALE
    # envs from ale_py.
    if isinstance(env_descriptor, str) and env_descriptor.startswith("ale_py:ALE/"):
        import ale_py

        gym.register_envs(ale_py)

    # Try creating a gym env. If this fails we can output a
    # decent error message.
    try:
        # If class provided, call constructor directly.
        if isinstance(env_descriptor, type):
            env = env_descriptor(env_context)
        else:
            env = gym.make(env_descriptor, **env_context)
    except gym.error.Error:
        raise EnvError(ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env_descriptor))

    return env
