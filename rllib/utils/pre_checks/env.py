"""Common pre-checks for all RLlib experiments."""
import logging
from copy import copy
from typing import TYPE_CHECKING, Set

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.error import ERR_MSG_OLD_GYM_API, UnsupportedSpaceException
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.util import log_once

if TYPE_CHECKING:
    from ray.rllib.env import MultiAgentEnv

logger = logging.getLogger(__name__)


@DeveloperAPI
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
        and hasattr(env, "_obs_space_in_preferred_format")
        and hasattr(env, "_action_space_in_preferred_format")
    ):
        if log_once("ma_env_super_ctor_called"):
            logger.warning(
                f"Your MultiAgentEnv {env} does not have some or all of the needed "
                "base-class attributes! Make sure you call `super().__init__()` from "
                "within your MutiAgentEnv's constructor. "
                "This will raise an error in the future."
            )
        return

    try:
        obs_and_infos = env.reset(seed=42, options={})
    except Exception as e:
        raise ValueError(
            ERR_MSG_OLD_GYM_API.format(
                env, "In particular, the `reset()` method seems to be faulty."
            )
        ) from e
    reset_obs, reset_infos = obs_and_infos

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

    sampled_action = env.action_space_sample(list(reset_obs.keys()))
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

    try:
        results = env.step(sampled_action)
    except Exception as e:
        raise ValueError(
            ERR_MSG_OLD_GYM_API.format(
                env, "In particular, the `step()` method seems to be faulty."
            )
        ) from e
    next_obs, reward, done, truncated, info = results

    _check_if_element_multi_agent_dict(env, next_obs, "step, next_obs")
    _check_if_element_multi_agent_dict(env, reward, "step, reward")
    _check_if_element_multi_agent_dict(env, done, "step, done")
    _check_if_element_multi_agent_dict(env, truncated, "step, truncated")
    _check_if_element_multi_agent_dict(env, info, "step, info", allow_common=True)
    _check_reward(
        {"dummy_env_id": reward}, base_env=True, agent_ids=env.get_agent_ids()
    )
    _check_done_and_truncated(
        {"dummy_env_id": done},
        {"dummy_env_id": truncated},
        base_env=True,
        agent_ids=env.get_agent_ids(),
    )
    _check_info({"dummy_env_id": info}, base_env=True, agent_ids=env.get_agent_ids())
    if not env.observation_space_contains(next_obs):
        error = (
            _not_contained_error("env.step(sampled_action)", "observation")
            + f":\n\n next_obs: {next_obs} \n\n sampled_obs: {sampled_obs}"
        )
        raise ValueError(error)


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


def _check_done_and_truncated(done, truncated, base_env=False, agent_ids=None):
    for what in ["done", "truncated"]:
        data = done if what == "done" else truncated
        if base_env:
            for _, multi_agent_dict in data.items():
                for agent_id, done_ in multi_agent_dict.items():
                    if not isinstance(done_, (bool, np.bool_)):
                        raise ValueError(
                            f"Your step function must return `{what}s` that are "
                            f"boolean. But instead was a {type(data)}"
                        )
                    if not (agent_id in agent_ids or agent_id == "__all__"):
                        error = (
                            f"Your `{what}s` dictionary must have agent ids that "
                            f"belong to the environment. Agent_ids recieved from "
                            f"env.get_agent_ids() are: {agent_ids}"
                        )
                        raise ValueError(error)
        elif not isinstance(data, (bool, np.bool_)):
            error = (
                f"Your step function must return a `{what}` that is a boolean. But "
                f"instead was a {type(data)}"
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
                if not (
                    agent_id in agent_ids
                    or agent_id == "__all__"
                    or agent_id == "__common__"
                ):
                    error = (
                        f"Your dones dictionary must have agent ids that belong to "
                        f"the environment. Agent_ids received from "
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


def _check_if_element_multi_agent_dict(
    env,
    element,
    function_string,
    base_env=False,
    allow_common=False,
):
    if not isinstance(element, dict):
        if base_env:
            error = (
                f"The element returned by {function_string} contains values "
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
    if allow_common:
        agent_ids.add("__common__")

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
                f"{list(env.get_agent_ids())}. You likely need to add the private "
                f"attribute `_agent_ids` to your env, which is a set containing the "
                f"ids of agents supported by your env."
            )
        raise ValueError(error)


def _find_offending_sub_space(space, value):
    """Returns error, value, and space when offending `space.contains(value)` fails.

    Returns only the offending sub-value/sub-space in case `space` is a complex Tuple
    or Dict space.

    Args:
        space: The gym.Space to check.
        value: The actual (numpy) value to check for matching `space`.

    Returns:
        Tuple consisting of 1) key-sequence of the offending sub-space or the empty
        string if `space` is not complex (Tuple or Dict), 2) the offending sub-space,
        3) the offending sub-space's dtype, 4) the offending sub-value, 5) the offending
        sub-value's dtype.

    .. testcode::
        :skipif: True

        path, space, space_dtype, value, value_dtype = _find_offending_sub_space(
            gym.spaces.Dict({
           -2.0, 1.5, (2, ), np.int8), np.array([-1.5, 3.0])
        )

    """
    if not isinstance(space, (gym.spaces.Dict, gym.spaces.Tuple)):
        return None, space, space.dtype, value, _get_type(value)

    structured_space = get_base_struct_from_space(space)

    def map_fn(p, s, v):
        if not s.contains(v):
            raise UnsupportedSpaceException((p, s, v))

    try:
        tree.map_structure_with_path(map_fn, structured_space, value)
    except UnsupportedSpaceException as e:
        space, value = e.args[0][1], e.args[0][2]
        return "->".join(e.args[0][0]), space, space.dtype, value, _get_type(value)

    # This is actually an error.
    return None, None, None, None, None


def _get_type(var):
    return var.dtype if hasattr(var, "dtype") else type(var)
