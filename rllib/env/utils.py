import logging
from typing import Type, Union

import gymnasium as gym

from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.wrappers.multi_agent_env_compatibility import (
    MultiAgentEnvCompatibility,
)
from ray.rllib.utils.error import (
    ERR_MSG_INVALID_ENV_DESCRIPTOR,
    ERR_MSG_OLD_GYM_API,
    EnvError,
)
from ray.rllib.utils.gym import check_old_gym_env
from ray.util import log_once
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
                "Could not import pyspiel! Pygame is not a dependency of RLlib "
                "and RLlib requires you to install pygame separately: "
                "`pip install pygame`."
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
    auto_wrap_old_gym_envs: bool = True,
) -> gym.Env:
    """Tries to create a gym env given an EnvContext object and descriptor.

    Note: This function tries to construct the env from a string descriptor
    only using possibly installed RL env packages (such as gym, pybullet_envs,
    vizdoomgym, etc..). These packages are no installation requirements for
    RLlib. In case you would like to support more such env packages, add the
    necessary imports and construction logic below.

    Args:
        env_context: The env context object to configure the env.
            Note that this is a config dict, plus the properties:
            `worker_index`, `vector_index`, and `remote`.
        env_descriptor: The env descriptor as a gym-registered string, e.g. CartPole-v1,
            ALE/MsPacman-v5, VizdoomBasic-v0, or CartPoleContinuousBulletEnv-v0.
            Alternatively, the gym.Env subclass to use.
        auto_wrap_old_gym_envs: Whether to auto-wrap old gym environments (using
            the pre 0.24 gym APIs, e.g. reset() returning single obs and no info
            dict). If True, RLlib will automatically wrap the given gym env class
            with the gym-provided compatibility wrapper (gym.wrappers.EnvCompatibility).
            If False, RLlib will produce a descriptive error on which steps to perform
            to upgrade to gymnasium (or to switch this flag to True).

    Returns:
        The actual gym environment object.

    Raises:
        gym.error.Error: If the env cannot be constructed.
    """
    # Allow for PyBullet or VizdoomGym envs to be used as well
    # (via string). This allows for doing things like
    # `env=CartPoleContinuousBulletEnv-v0` or
    # `env=VizdoomBasic-v0`.
    try:
        import pybullet_envs

        pybullet_envs.getList()
    except (AttributeError, ModuleNotFoundError, ImportError):
        pass
    try:
        import vizdoomgym

        vizdoomgym.__name__  # trick LINTer.
    except (ModuleNotFoundError, ImportError):
        pass

    # Try creating a gym env. If this fails we can output a
    # decent error message.
    try:
        # If class provided, call constructor directly.
        if isinstance(env_descriptor, type):
            env = env_descriptor(env_context)
        else:
            env = gym.make(env_descriptor, **env_context)
        # If we are dealing with an old gym-env API, use the provided compatibility
        # wrapper.
        if auto_wrap_old_gym_envs:
            try:
                # Call the env's reset() method to check for the env using the old
                # gym (reset doesn't take `seed` and `options` args and returns only
                # the initial observations) or new gymnasium APIs (reset takes `seed`
                # and `options` AND returns observations and infos).
                obs_and_infos = env.reset(seed=None, options={})
                # Check return values for correct gymnasium .
                check_old_gym_env(reset_results=obs_and_infos)
            # TypeError for `reset()` not accepting seed/options.
            # ValueError for `check_old_gym_env` raising error if return values
            # incorrect.
            except Exception:
                if log_once("auto_wrap_gym_api"):
                    logger.warning(
                        "`config.auto_wrap_old_gym_envs` is activated AND you seem to "
                        "have provided an old gym-API environment. RLlib will therefore"
                        " try to auto-fix the following error. However, please "
                        "consider switching over to the new `gymnasium` APIs:\n"
                        + ERR_MSG_OLD_GYM_API
                    )
                # Multi-agent case.
                if isinstance(env, MultiAgentEnv):
                    env = MultiAgentEnvCompatibility(env)
                # Single agent (gymnasium.Env) case.
                else:
                    env = gym.wrappers.EnvCompatibility(env)
                # Repeat the checks, now everything should work.
                obs_and_infos = env.reset(seed=None, options={})
                check_old_gym_env(reset_results=obs_and_infos)
    except gym.error.Error:
        raise EnvError(ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env_descriptor))

    return env
