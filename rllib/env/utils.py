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
        The tfp module.

    Raises:
        ImportError: If error=True and tfp is not installed.
    """
    try:
        import pyspiel

        return pyspiel
    except ImportError as e:
        if error:
            raise ImportError(
                "Could not import pyspiel! Pygame is not a dependency of RLlib "
                "and Rllib requires you to install pygame separately: "
                "`pip install pygame`."
            )
        return None


def _gym_env_creator(env_context: EnvContext, env_descriptor: str) -> gym.Env:
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
            return gym.make(env_descriptor, **env_context)
    except gym.error.Error:
        raise EnvError(ERR_MSG_INVALID_ENV_DESCRIPTOR.format(env_descriptor))
