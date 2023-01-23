import gymnasium as gym

from ray.rllib.env.env_context import EnvContext
from ray.rllib.utils.error import ERR_MSG_INVALID_ENV_DESCRIPTOR, EnvError
from ray.util.annotations import PublicAPI


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
    env_context: EnvContext, env_descriptor: str, gym_registry: dict
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
        env_descriptor: The env descriptor, e.g. CartPole-v1,
            ALE/MsPacman-v5, VizdoomBasic-v0, or
            CartPoleContinuousBulletEnv-v0.

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
        # Special case: Atari not supported by gymnasium yet -> Need to use their
        # GymV26 compatibility wrapper class.
        # TODO(sven): Remove this if-block once gymnasium fully supports Atari envs.
        if env_descriptor.startswith("ALE/"):
            return gym.make(
                "GymV26Environment-v0",
                env_id=env_descriptor,
                make_kwargs=env_context,
            )
        else:
            # `env_descriptor` not registered. Try to re-register with the given
            # registry.
            if (
                env_descriptor not in gym.envs.registry
                and env_descriptor in gym_registry
            ):
                r = gym_registry[env_descriptor]
                gym.register(
                    env_descriptor,
                    r.entry_point,
                    reward_threshold=r.reward_threshold,
                    nondeterministic=r.nondeterministic,
                    max_episode_steps=r.max_episode_steps,
                    order_enforce=r.order_enforce,
                    autoreset=r.autoreset,
                    apply_api_compatibility=r.apply_api_compatibility,
                    disable_env_checker=True,
                    kwargs=r.kwargs,
                )
            return gym.make(env_descriptor, **env_context)
    except gym.error.Error:
        raise EnvError(ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env_descriptor))


def is_wrapped_multi_agent_env(env):
    from ray.rllib.env.multi_agent_env import MultiAgentEnv

    return isinstance(env, gym.Env) and isinstance(env.unwrapped, MultiAgentEnv)
