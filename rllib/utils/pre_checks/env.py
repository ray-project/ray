"""Common pre-checks for all RLlib experiments."""
import logging
from typing import TYPE_CHECKING, Set

import gym
from ray.rllib.utils.typing import EnvType

if TYPE_CHECKING:
    from ray.rllib.env import BaseEnv, MultiAgentEnv

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
            "MultiAgentEnv, VectorEnv, RemoteBaseEnv")

    if isinstance(env, MultiAgentEnv):
        check_multiagent_environments(env)
    elif isinstance(env, gym.Env):
        check_gym_environments(env)
    elif isinstance(env, BaseEnv):
        check_base_env(env)
    else:
        logger.warning("Env checking isn't implemented for VectorEnvs or "
                       "RemoteBaseEnvs.")


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
    _check_done(done)
    _check_reward(reward)
    _check_info(info)


def check_multiagent_environments(env: "MultiAgentEnv") -> None:
    """Checking for common errors in RLlib MultiAgentEnvs.

    Args:
        env: The env to be checked.

    """
    from ray.rllib.env import MultiAgentEnv
    if not isinstance(env, MultiAgentEnv):
        raise ValueError("The passed env is not a MultiAgentEnv.")

    reset_obs = env.reset()
    sampled_obs = env.observation_space_sample()
    _check_if_element_multi_agent_dict(env, reset_obs, "reset()")
    _check_if_element_multi_agent_dict(env, sampled_obs,
                                       "env.observation_space_sample()")

    try:
        env.observation_space_contains(reset_obs)
    except Exception as e:
        raise ValueError("Your observation_space_contains function has some "
                         "error ") from e

    if not env.observation_space_contains(reset_obs):
        error = (
            _not_contained_error("env.reset", "observation") +
            f"\n\n reset_obs: {reset_obs}\n\n env.observation_space_sample():"
            f" {sampled_obs}\n\n ")
        raise ValueError(error)

    if not env.observation_space_contains(sampled_obs):
        error = (
            _not_contained_error("observation_space_sample", "observation") +
            f"\n\n env.observation_space_sample():"
            f" {sampled_obs}\n\n ")
        raise ValueError(error)

    sampled_action = env.action_space_sample()
    _check_if_element_multi_agent_dict(env, sampled_action,
                                       "action_space_sample")
    try:
        env.action_space_contains(sampled_action)
    except Exception as e:
        raise ValueError("Your action_space_contains function has some "
                         "error ") from e

    if not env.action_space_contains(sampled_action):
        error = (_not_contained_error("action_space_sample", "action") +
                 "\n\n sampled_action {sampled_action}\n\n")
        raise ValueError(error)

    next_obs, reward, done, info = env.step(sampled_action)
    _check_if_element_multi_agent_dict(env, next_obs, "step(sampled_action)")
    _check_reward(reward)
    _check_done(done)
    _check_info(info)
    if not env.observation_space_contains(next_obs):
        error = (
            _not_contained_error("env.step(sampled_action)", "observation") +
            ":\n\n next_obs: {next_obs} \n\n sampled_obs: {sampled_obs}")
        raise ValueError(error)


def check_base_env(env: "BaseEnv") -> None:
    """Checking for common errors in RLlib BaseEnvs.

    Args:
        env: The env to be checked.

    """
    from ray.rllib.env import BaseEnv
    if not isinstance(env, BaseEnv):
        raise ValueError("The passed env is not a BaseEnv.")

    reset_obs = env.try_reset()
    sampled_obs = env.observation_space_sample()
    _check_if_multi_env_dict(env, reset_obs, "try_reset")
    _check_if_multi_env_dict(env, sampled_obs, "observation_space_sample()")

    try:
        env.observation_space_contains(reset_obs)
    except Exception as e:
        raise ValueError("Your observation_space_contains function has some "
                         "error ") from e

    if not env.observation_space_contains(reset_obs):
        error = (_not_contained_error("try_reset", "observation") +
                 f": \n\n reset_obs: {reset_obs}\n\n "
                 f"env.observation_space_sample(): {sampled_obs}\n\n ")
        raise ValueError(error)

    if not env.observation_space_contains(sampled_obs):
        error = (
            _not_contained_error("observation_space_sample", "observation") +
            f": \n\n sampled_obs: {sampled_obs}\n\n ")
        raise ValueError(error)

    sampled_action = env.action_space_sample()
    try:
        env.action_space_contains(sampled_action)
    except Exception as e:
        raise ValueError("Your action_space_contains function has some "
                         "error ") from e
    if not env.action_space_contains(sampled_action):
        error = (_not_contained_error("action_space_sample", "action") +
                 f": \n\n sampled_action {sampled_action}\n\n")
        raise ValueError(error)
    _check_if_multi_env_dict(env, sampled_action, "action_space_sample()")

    env.send_actions(sampled_action)

    next_obs, reward, done, info, _ = env.poll()
    _check_if_multi_env_dict(env, next_obs, "step, next_obs")
    _check_if_multi_env_dict(env, reward, "step, reward")
    _check_if_multi_env_dict(env, done, "step, done")
    _check_if_multi_env_dict(env, info, "step, info")

    if not env.observation_space_contains(next_obs):
        error = (
            _not_contained_error("poll", "observation") +
            f": \n\n reset_obs: {reset_obs}\n\n env.step():{next_obs}\n\n")
        raise ValueError(error)

    _check_reward(reward, base_env=True)
    _check_done(done, base_env=True)
    _check_info(info, base_env=True)


def _check_reward(reward, base_env=False):
    if base_env:
        for _, multi_agent_dict in reward.items():
            for _, rew in multi_agent_dict.items():
                assert isinstance(rew, (float, int)), \
                    "Your step function must return a rewards that are" \
                    f" integer or float. reward: {rew}"
    else:
        assert isinstance(reward, (float, int)), \
            "Your step function must return a reward that is integer or float."


def _check_done(done, base_env=False):
    if base_env:
        for _, multi_agent_dict in done.items():
            for _, done_ in multi_agent_dict.items():
                assert isinstance(done_, bool), \
                    "Your step function must return a done that is boolean. " \
                    f"element: {done_}"
    else:
        assert isinstance(
            done, bool), "Your step function must return a done that is a " \
                         "boolean."


def _check_info(info, base_env=False):
    if base_env:
        for _, multi_agent_dict in info.items():
            for _, inf in multi_agent_dict.items():
                assert isinstance(inf, dict), \
                    "Your step function must return a info that is a dict. " \
                    f"element: {inf}"
    else:
        assert isinstance(
            info,
            dict), "Your step function must return a info that is a dict."


def _not_contained_error(func_name, _type):
    _error = \
        (f"The {_type} collected from {func_name} was not contained within"
         f" your env's {_type} space. Its possible that there was a type"
         f"mismatch (for example {_type}s of np.float32 and a space of"
         f"np.float64 {_type}s), or that one of the sub-{_type}s was"
         f"out of bounds")
    return _error


def _check_if_multi_env_dict(env, element, function_string):
    if not isinstance(element, dict):
        raise ValueError(
            f"The element returned by {function_string} is not a "
            f"MultiEnvDict. Instead, it is of type: {type(element)}")
    env_ids = env.get_sub_environments(as_dict=True).keys()
    if not all(k in env_ids for k in element):
        raise ValueError(f"The element returned by {function_string} "
                         f"has dict keys that don't correspond to "
                         f"environment ids for this env "
                         f"{list(env_ids)}")
    for _, multi_agent_dict in element.items():
        _check_if_element_multi_agent_dict(
            env, multi_agent_dict, function_string, base_env=True)


def _check_if_element_multi_agent_dict(env,
                                       element,
                                       function_string,
                                       base_env=False):
    if not isinstance(element, dict):
        if base_env:
            error = (f"The element returned by {function_string} has values "
                     f"that are not MultiAgentDicts. Instead, they are of "
                     f"type: {type(element)}")
        else:
            error = (f"The element returned by {function_string} is not a "
                     f"MultiAgentDict. Instead, it is of type: "
                     f" {type(element)}")
        raise ValueError(error)
    agent_ids: Set = env.get_agent_ids()
    agent_ids.add("__all__")
    if not all(k in agent_ids for k in element):
        if base_env:
            error = (f"The element returned by {function_string} has agent_ids"
                     f" that are not the names of the agents in the env."
                     f"agent_ids in this\nMultiEnvDict:"
                     f" {list(element.keys())}\nAgent_ids in this env:"
                     f"{list(env.get_agent_ids())}")
        else:
            error = (f"The element returned by {function_string} has agent_ids"
                     f" that are not the names of the agents in the env. "
                     f"\nAgent_ids in this MultiAgentDict: "
                     f"{list(element.keys())}\nAgent_ids in this env:"
                     f"{list(env.get_agent_ids())}")
        raise ValueError(error)
