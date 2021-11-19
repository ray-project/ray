"""Common pre-checks for all RLlib experiments."""
import logging

import gym
from ray.rllib.utils.typing import EnvType

logger = logging.getLogger(__name__)


def check_env(env: EnvType) -> None:
    """Run pre-checks on env that uncover common errors in environments.

    Args:
        env: Environment to be checked.

    Raises:
        ValueError: If env is not an instance of SUPPORTED_ENVIRONMENT_TYPES.
        ValueError: See check_gym_env docstring for details.
    """
    from ray.rllib.env import BaseEnv, MultiAgentEnv, RemoteBaseEnv, \
        VectorEnv

    if not isinstance(
            env, (BaseEnv, gym.Env, MultiAgentEnv, RemoteBaseEnv, VectorEnv)):
        raise ValueError(
            "Env must be one of the supported types: BaseEnv, gym.Env, "
            "MultiAgentEnv, VectorEnv, RemoteVectorEnv")

    if isinstance(env, gym.Env):
        check_gym_environments(env)


def check_gym_environments(env: gym.Env) -> None:
    """Checking for common errors in gym environments.

    Args:
        env: Environment to be checked.

    Warning:
        If env has no attribute spec with a sub attribute,
            max_episode_steps.

    Raises:
        AttributeError: If env has no observation space.
        AttributeError: If env has no action space.
        ValueError: Observation space must be a gym.spaces.Space.
        ValueError: Action space must be a gym.spaces.Space.
        ValueError: Observation sampled from observation space must be
            contained in the observation space.
        ValueError: Action sampled from action space must be
            contained in the observation space.
        ValueError: If env cannot be resetted.
        ValueError: If an observation collected from a call to env.reset().
            is not contained in the observation_space.
        ValueError: If env cannot be stepped via a call to env.step().
        ValueError: If the observation collected from env.step() is not
            contained in the observation_space.
        AssertionError: If env.step() returns a reward that is not an
            int or float.
        AssertionError: IF env.step() returns a done that is not a bool.
        AssertionError: If env.step() returns an env_info that is not a dict.
    """

    # check that env has observation and action spaces
    if not hasattr(env, "observation_space"):
        raise AttributeError("Env must have observation_space.")
    if not hasattr(env, "action_space"):
        raise AttributeError("Env must have action_space.")

    # check that observation and action spaces are gym.spaces
    if not isinstance(env.observation_space, gym.spaces.Space):
        raise ValueError("Observation space must be a gym.space")
    if not isinstance(env.action_space, gym.spaces.Space):
        raise ValueError("Action space must be a gym.space")

    # raise a warning if there isn't a max_episode_steps attribute
    if not hasattr(env, "spec") or not hasattr(env.spec, "max_episode_steps"):
        logger.warning("Your env doesn't have a .spec.max_episode_steps "
                       "attribute. This is fine if you have set 'horizon' "
                       "in your config dictionary, or `soft_horizon`. "
                       "However, if you haven't, 'horizon' will default "
                       "to infinity, and your environment will not be "
                       "reset.")
    # check if sampled actions and observations are contained within their
    # respective action and observation spaces.

    def contains_error(action_or_observation, sample, space):
        string_type = "observation" if not action_or_observation else \
            "action"
        sample_type = get_type(sample)
        _space_type = space.dtype
        ret = (f"A sampled {string_type} from your env wasn't contained "
               f"within your env's {string_type} space. Its possible that "
               f"there was a type mismatch, or that one of the "
               f"sub-{string_type} was out of bounds:\n\nsampled_obs: "
               f"{sample}\nenv.{string_type}_space: {space}"
               f"\nsampled_obs's dtype: {sample_type}"
               f"\nenv.{sample_type}'s dtype: {_space_type}")
        return ret

    def get_type(var):
        return var.dtype if hasattr(var, "dtype") else type(var)

    sampled_action = env.action_space.sample()
    sampled_observation = env.observation_space.sample()
    if not env.observation_space.contains(sampled_observation):
        raise ValueError(
            contains_error(False, sampled_observation, env.observation_space))
    if not env.action_space.contains(sampled_action):
        raise ValueError(
            contains_error(True, sampled_action, env.action_space))

    # check if observation generated from stepping the environment is
    # contained within the observation space
    reset_obs = env.reset()
    if not env.observation_space.contains(reset_obs):
        reset_obs_type = get_type(reset_obs)
        space_type = env.observation_space.dtype
        error = (
            f"The observation collected from env.reset() was not  "
            f"contained within your env's observation space. Its possible "
            f"that There was a type mismatch, or that one of the "
            f"sub-observations  was out of bounds: \n\n reset_obs: "
            f"{reset_obs}\n\n env.observation_space: "
            f"{env.observation_space}\n\n reset_obs's dtype: "
            f"{reset_obs_type}\n\n env.observation_space's dtype: "
            f"{space_type}")
        raise ValueError(error)
    # check if env.step can run, and generates observations rewards, done
    # signals and infos that are within their respective spaces and are of
    # the correct dtypes
    next_obs, reward, done, info = env.step(sampled_action)
    if not env.observation_space.contains(next_obs):
        next_obs_type = get_type(next_obs)
        space_type = env.observation_space.dtype
        error = (
            f"The observation collected from env.step(sampled_action) was "
            f"not contained within your env's observation space. Its "
            f"possible that There was a type mismatch, or that one of the "
            f"sub-observations was out of bounds:\n\n next_obs: {next_obs}"
            f"\n\n env.observation_space: {env.observation_space}"
            f"\n\n next_obs's dtype: {next_obs_type}"
            f"\n\n env.observation_space's dtype: {space_type}")
        raise ValueError(error)
    assert isinstance(reward, (float, int)), \
        "Your step function must return a reward that is integer or float."
    assert isinstance(
        done, bool), "Your step function must return a done that is a boolean."
    assert isinstance(
        info, dict), "Your step function must return a info that is a dict."
