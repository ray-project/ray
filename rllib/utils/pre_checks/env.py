"""Common pre-checks for all RLlib experiments."""
from copy import copy
import logging
import gym
import numpy as np
import traceback
from typing import TYPE_CHECKING, Set

from ray.actor import ActorHandle
from ray.rllib.utils.spaces.space_utils import convert_element_to_space_type
from ray.rllib.utils.typing import EnvType
from ray.util import log_once

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
    from ray.rllib.env import (
        BaseEnv,
        MultiAgentEnv,
        RemoteBaseEnv,
        VectorEnv,
        ExternalMultiAgentEnv,
        ExternalEnv,
    )

    if hasattr(env, "_skip_env_checking") and env._skip_env_checking:
        # This is a work around for some environments that we already have in RLlb
        # that we want to skip checking for now until we have the time to fix them.
        logger.warning("Skipping env checking for this experiment")
        return

    try:
        if not isinstance(
            env,
            (
                BaseEnv,
                gym.Env,
                MultiAgentEnv,
                RemoteBaseEnv,
                VectorEnv,
                ExternalMultiAgentEnv,
                ExternalEnv,
                ActorHandle,
            ),
        ):
            raise ValueError(
                "Env must be one of the supported types: BaseEnv, gym.Env, "
                "MultiAgentEnv, VectorEnv, RemoteBaseEnv, ExternalMultiAgentEnv, "
                f"ExternalEnv, but instead was a {type(env)}"
            )
        if isinstance(env, MultiAgentEnv):
            check_multiagent_environments(env)
        elif isinstance(env, gym.Env):
            check_gym_environments(env)
        elif isinstance(env, BaseEnv):
            check_base_env(env)
        else:
            logger.warning(
                "Env checking isn't implemented for VectorEnvs, RemoteBaseEnvs, "
                "ExternalMultiAgentEnv,or ExternalEnvs or Environments that are "
                "Ray actors"
            )
    except Exception:
        actual_error = traceback.format_exc()
        raise ValueError(
            f"{actual_error}\n"
            "The above error has been found in your environment! "
            "We've added a module for checking your custom environments. It "
            "may cause your experiment to fail if your environment is not set up"
            "correctly. You can disable this behavior by setting "
            "`disable_env_checking=True` in your config "
            "dictionary. You can run the environment checking module "
            "standalone by calling ray.rllib.utils.check_env([env])."
        )


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
        logger.warning(
            "Your env doesn't have a .spec.max_episode_steps "
            "attribute. This is fine if you have set 'horizon' "
            "in your config dictionary, or `soft_horizon`. "
            "However, if you haven't, 'horizon' will default "
            "to infinity, and your environment will not be "
            "reset."
        )
    # check if sampled actions and observations are contained within their
    # respective action and observation spaces.

    def get_type(var):
        return var.dtype if hasattr(var, "dtype") else type(var)

    sampled_action = env.action_space.sample()
    sampled_observation = env.observation_space.sample()
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
            f"{space_type}"
        )
        temp_sampled_reset_obs = convert_element_to_space_type(
            reset_obs, sampled_observation
        )
        if not env.observation_space.contains(temp_sampled_reset_obs):
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
            f"\n\n env.observation_space's dtype: {space_type}"
        )
        temp_sampled_next_obs = convert_element_to_space_type(
            next_obs, sampled_observation
        )
        if not env.observation_space.contains(temp_sampled_next_obs):
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
    elif not (
        hasattr(env, "observation_space")
        and hasattr(env, "action_space")
        and hasattr(env, "_agent_ids")
        and hasattr(env, "_spaces_in_preferred_format")
    ):
        if log_once("ma_env_super_ctor_called"):
            logger.warning(
                f"Your MultiAgentEnv {env} does not have some or all of the needed "
                "base-class attributes! Make sure you call `super().__init__` from "
                "within your MutiAgentEnv's constructor. "
                "This will raise an error in the future."
            )
        return

    reset_obs = env.reset()
    sampled_obs = env.observation_space_sample()
    _check_if_element_multi_agent_dict(env, reset_obs, "reset()")
    _check_if_element_multi_agent_dict(
        env, sampled_obs, "env.observation_space_sample()"
    )

    try:
        env.observation_space_contains(reset_obs)
    except Exception as e:
        raise ValueError(
            "Your observation_space_contains function has some error "
        ) from e

    if not env.observation_space_contains(reset_obs):
        error = (
            _not_contained_error("env.reset", "observation")
            + f"\n\n reset_obs: {reset_obs}\n\n env.observation_space_sample():"
            f" {sampled_obs}\n\n "
        )
        raise ValueError(error)

    if not env.observation_space_contains(sampled_obs):
        error = (
            _not_contained_error("observation_space_sample", "observation")
            + f"\n\n env.observation_space_sample():"
            f" {sampled_obs}\n\n "
        )
        raise ValueError(error)

    sampled_action = env.action_space_sample()
    _check_if_element_multi_agent_dict(env, sampled_action, "action_space_sample")
    try:
        env.action_space_contains(sampled_action)
    except Exception as e:
        raise ValueError("Your action_space_contains function has some error ") from e

    if not env.action_space_contains(sampled_action):
        error = (
            _not_contained_error("action_space_sample", "action")
            + f"\n\n sampled_action {sampled_action}\n\n"
        )
        raise ValueError(error)

    next_obs, reward, done, info = env.step(sampled_action)
    _check_if_element_multi_agent_dict(env, next_obs, "step, next_obs")
    _check_if_element_multi_agent_dict(env, reward, "step, reward")
    _check_if_element_multi_agent_dict(env, done, "step, done")
    _check_if_element_multi_agent_dict(env, info, "step, info")
    _check_reward(
        {"dummy_env_id": reward}, base_env=True, agent_ids=env.get_agent_ids()
    )
    _check_done({"dummy_env_id": done}, base_env=True, agent_ids=env.get_agent_ids())
    _check_info({"dummy_env_id": info}, base_env=True, agent_ids=env.get_agent_ids())
    if not env.observation_space_contains(next_obs):
        error = (
            _not_contained_error("env.step(sampled_action)", "observation")
            + f":\n\n next_obs: {next_obs} \n\n sampled_obs: {sampled_obs}"
        )
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
        raise ValueError(
            "Your observation_space_contains function has some error "
        ) from e

    if not env.observation_space_contains(reset_obs):
        error = (
            _not_contained_error("try_reset", "observation")
            + f": \n\n reset_obs: {reset_obs}\n\n "
            f"env.observation_space_sample(): {sampled_obs}\n\n "
        )
        raise ValueError(error)

    if not env.observation_space_contains(sampled_obs):
        error = (
            _not_contained_error("observation_space_sample", "observation")
            + f": \n\n sampled_obs: {sampled_obs}\n\n "
        )
        raise ValueError(error)

    sampled_action = env.action_space_sample()
    try:
        env.action_space_contains(sampled_action)
    except Exception as e:
        raise ValueError("Your action_space_contains function has some error ") from e
    if not env.action_space_contains(sampled_action):
        error = (
            _not_contained_error("action_space_sample", "action")
            + f": \n\n sampled_action {sampled_action}\n\n"
        )
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
            _not_contained_error("poll", "observation")
            + f": \n\n reset_obs: {reset_obs}\n\n env.step():{next_obs}\n\n"
        )
        raise ValueError(error)

    _check_reward(reward, base_env=True, agent_ids=env.get_agent_ids())
    _check_done(done, base_env=True, agent_ids=env.get_agent_ids())
    _check_info(info, base_env=True, agent_ids=env.get_agent_ids())


def _check_reward(reward, base_env=False, agent_ids=None):
    if base_env:
        for _, multi_agent_dict in reward.items():
            for agent_id, rew in multi_agent_dict.items():
                if not (
                    np.isreal(rew)
                    and not isinstance(rew, bool)
                    and (
                        np.isscalar(rew)
                        or (isinstance(rew, np.ndarray) and rew.shape == ())
                    )
                ):
                    error = (
                        "Your step function must return rewards that are"
                        f" integer or float. reward: {rew}. Instead it was a "
                        f"{type(rew)}"
                    )
                    raise ValueError(error)
                if not (agent_id in agent_ids or agent_id == "__all__"):
                    error = (
                        f"Your reward dictionary must have agent ids that belong to "
                        f"the environment. Agent_ids recieved from "
                        f"env.get_agent_ids() are: {agent_ids}"
                    )
                    raise ValueError(error)
    elif not (
        np.isreal(reward)
        and not isinstance(reward, bool)
        and (
            np.isscalar(reward)
            or (isinstance(reward, np.ndarray) and reward.shape == ())
        )
    ):
        error = (
            "Your step function must return a reward that is integer or float. "
            "Instead it was a {}".format(type(reward))
        )
        raise ValueError(error)


def _check_done(done, base_env=False, agent_ids=None):
    if base_env:
        for _, multi_agent_dict in done.items():
            for agent_id, done_ in multi_agent_dict.items():
                if not isinstance(done_, (bool, np.bool, np.bool_)):
                    raise ValueError(
                        "Your step function must return dones that are boolean. But "
                        f"instead was a {type(done)}"
                    )
                if not (agent_id in agent_ids or agent_id == "__all__"):
                    error = (
                        f"Your dones dictionary must have agent ids that belong to "
                        f"the environment. Agent_ids recieved from "
                        f"env.get_agent_ids() are: {agent_ids}"
                    )
                    raise ValueError(error)
    elif not isinstance(done, (bool, np.bool, np.bool_)):
        error = (
            "Your step function must return a done that is a boolean. But instead "
            f"was a {type(done)}"
        )
        raise ValueError(error)


def _check_info(info, base_env=False, agent_ids=None):
    if base_env:
        for _, multi_agent_dict in info.items():
            for agent_id, inf in multi_agent_dict.items():
                if not isinstance(inf, dict):
                    raise ValueError(
                        "Your step function must return infos that are a dict. "
                        f"instead was a {type(inf)}: element: {inf}"
                    )
                if not (agent_id in agent_ids or agent_id == "__all__"):
                    error = (
                        f"Your dones dictionary must have agent ids that belong to "
                        f"the environment. Agent_ids recieved from "
                        f"env.get_agent_ids() are: {agent_ids}"
                    )
                    raise ValueError(error)
    elif not isinstance(info, dict):
        error = (
            "Your step function must return a info that "
            f"is a dict. element type: {type(info)}. element: {info}"
        )
        raise ValueError(error)


def _not_contained_error(func_name, _type):
    _error = (
        f"The {_type} collected from {func_name} was not contained within"
        f" your env's {_type} space. Its possible that there was a type"
        f"mismatch (for example {_type}s of np.float32 and a space of"
        f"np.float64 {_type}s), or that one of the sub-{_type}s was"
        f"out of bounds"
    )
    return _error


def _check_if_multi_env_dict(env, element, function_string):
    if not isinstance(element, dict):
        raise ValueError(
            f"The element returned by {function_string} is not a "
            f"MultiEnvDict. Instead, it is of type: {type(element)}"
        )
    env_ids = env.get_sub_environments(as_dict=True).keys()
    if not all(k in env_ids for k in element):
        raise ValueError(
            f"The element returned by {function_string} "
            f"has dict keys that don't correspond to "
            f"environment ids for this env "
            f"{list(env_ids)}"
        )
    for _, multi_agent_dict in element.items():
        _check_if_element_multi_agent_dict(
            env, multi_agent_dict, function_string, base_env=True
        )


def _check_if_element_multi_agent_dict(env, element, function_string, base_env=False):
    if not isinstance(element, dict):
        if base_env:
            error = (
                f"The element returned by {function_string} has values "
                f"that are not MultiAgentDicts. Instead, they are of "
                f"type: {type(element)}"
            )
        else:
            error = (
                f"The element returned by {function_string} is not a "
                f"MultiAgentDict. Instead, it is of type: "
                f" {type(element)}"
            )
        raise ValueError(error)
    agent_ids: Set = copy(env.get_agent_ids())
    agent_ids.add("__all__")

    if not all(k in agent_ids for k in element):
        if base_env:
            error = (
                f"The element returned by {function_string} has agent_ids"
                f" that are not the names of the agents in the env."
                f"agent_ids in this\nMultiEnvDict:"
                f" {list(element.keys())}\nAgent_ids in this env:"
                f"{list(env.get_agent_ids())}"
            )
        else:
            error = (
                f"The element returned by {function_string} has agent_ids"
                f" that are not the names of the agents in the env. "
                f"\nAgent_ids in this MultiAgentDict: "
                f"{list(element.keys())}\nAgent_ids in this env:"
                f"{list(env.get_agent_ids())}. You likley need to add the private "
                f"attribute `_agent_ids` to your env, which is a set containing the "
                f"ids of agents supported by your env."
            )
        raise ValueError(error)
