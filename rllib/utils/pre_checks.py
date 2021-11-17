"""Common pre-checks for all RLlib experiments."""
import logging

from ray.rllib.env import BaseEnv, MultiAgentEnv, RemoteVectorEnv, VectorEnv
import gym

logger = logging.getLogger(__name__)


def check_env(
        env: [BaseEnv, gym.Env, MultiAgentEnv, RemoteVectorEnv,
              VectorEnv]) -> None:
    """Run pre-checks on env that uncover common errors in environments.

    Args:
        env: Environment to be checked.

    Raises:
        ValueError: If env is not one of the supported types.
    """
    supported_types = (BaseEnv, gym.Env, MultiAgentEnv, RemoteVectorEnv,
                       VectorEnv)
    if not isinstance(env, supported_types):
        raise ValueError(
            "Env must be one of the supported types: BaseEnv, gym.Env, "
            "MultiAgentEnv, VectorEnv, RemoteVectorEnv")

    def check_gym_environments(_env: gym.Env) -> None:
        """Checking for common errors in gym environments."""

        # check that env has observation and action spaces
        if not hasattr(_env, "observation_space"):
            raise ValueError("Env must have observation_space.")
        if not hasattr(_env, "action_space"):
            raise ValueError("Env must have action_space.")
        if not hasattr(_env, "spec"):
            raise ValueError("Env must have a spec attribute")

        # check that observation and action spaces are gym.spaces
        if not isinstance(_env.observation_space, gym.spaces.Space):
            raise ValueError("Observation space must be a gym.space")
        if not isinstance(_env.action_space, gym.spaces.Space):
            raise ValueError("Action space must be a gym.space")
        if (not hasattr(_env, "spec")
                and not hasattr(_env.spec, "max_episode_steps")):
            logger.warning("Your env doesn't have a .spec.max_episode_steps "
                           "attribute. This is fine if you have set 'horizon' "
                           "in your config dictionary, or `soft_horizon`. "
                           "However, if you haven't, 'horizon' will default "
                           "to infinity, and your environment will not be "
                           "reset.")
