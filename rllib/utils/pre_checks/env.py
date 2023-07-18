"""Common pre-checks for all RLlib experiments."""
import logging
import traceback
from copy import copy
from typing import TYPE_CHECKING, Optional, Set, Union

import numpy as np
import tree  # pip install dm_tree

from ray.actor import ActorHandle
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.error import ERR_MSG_OLD_GYM_API, UnsupportedSpaceException
from ray.rllib.utils.gym import check_old_gym_env, try_import_gymnasium_and_gym
from ray.rllib.utils.spaces.space_utils import (
    convert_element_to_space_type,
    get_base_struct_from_space,
)
from ray.rllib.utils.typing import EnvType
from ray.util import log_once

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.env import BaseEnv, MultiAgentEnv, VectorEnv

logger = logging.getLogger(__name__)

gym, old_gym = try_import_gymnasium_and_gym()


@DeveloperAPI
def check_env(env: EnvType, config: Optional["AlgorithmConfig"] = None) -> None:
    """Run pre-checks on env that uncover common errors in environments.

    Args:
        env: Environment to be checked.
        config: Additional checks config.

    Raises:
        ValueError: If env is not an instance of SUPPORTED_ENVIRONMENT_TYPES.
        ValueError: See check_gym_env docstring for details.
    """
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.env import (
        BaseEnv,
        ExternalEnv,
        ExternalMultiAgentEnv,
        MultiAgentEnv,
        RemoteBaseEnv,
        VectorEnv,
    )

    if hasattr(env, "_skip_env_checking") and env._skip_env_checking:
        # This is a work around for some environments that we already have in RLlb
        # that we want to skip checking for now until we have the time to fix them.
        if log_once("skip_env_checking"):
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
        ) and (not old_gym or not isinstance(env, old_gym.Env)):
            raise ValueError(
                "Env must be of one of the following supported types: BaseEnv, "
                "gymnasium.Env, gym.Env, "
                "MultiAgentEnv, VectorEnv, RemoteBaseEnv, ExternalMultiAgentEnv, "
                f"ExternalEnv, but instead is of type {type(env)}."
            )

        if isinstance(env, MultiAgentEnv):
            check_multiagent_environments(env)
        elif isinstance(env, VectorEnv):
            check_vector_env(env)
        elif isinstance(env, gym.Env) or old_gym and isinstance(env, old_gym.Env):
            check_gym_environments(env, AlgorithmConfig() if config is None else config)
        elif isinstance(env, BaseEnv):
            check_base_env(env)
        else:
            logger.warning(
                "Env checking isn't implemented for RemoteBaseEnvs, "
                "ExternalMultiAgentEnv, ExternalEnvs or environments that are "
                "Ray actors."
            )
    except Exception:
        actual_error = traceback.format_exc()
        raise ValueError(
            f"{actual_error}\n"
            "The above error has been found in your environment! "
            "We've added a module for checking your custom environments. It "
            "may cause your experiment to fail if your environment is not set up "
            "correctly. You can disable this behavior via calling `config."
            "environment(disable_env_checking=True)`. You can run the "
            "environment checking module standalone by calling "
            "ray.rllib.utils.check_env([your env])."
        )


@DeveloperAPI
def check_gym_environments(
    env: Union[gym.Env, "old_gym.Env"], config: "AlgorithmConfig"
) -> None:
    """Checking for common errors in a gymnasium/gym environments.

    Args:
        env: Environment to be checked.
        config: Additional checks config.

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

    # Check for old gym.Env.
    if old_gym and isinstance(env, old_gym.Env):
        raise ValueError(ERR_MSG_OLD_GYM_API.format(env, ""))

    # Check that env has observation and action spaces.
    if not hasattr(env, "observation_space"):
        raise AttributeError("Env must have observation_space.")
    if not hasattr(env, "action_space"):
        raise AttributeError("Env must have action_space.")

    # check that observation and action spaces are gym.spaces
    if not isinstance(env.observation_space, gym.spaces.Space):
        raise ValueError("Observation space must be a gymnasium.Space!")
    if not isinstance(env.action_space, gym.spaces.Space):
        raise ValueError("Action space must be a gymnasium.Space!")

    # Raise a warning if there isn't a max_episode_steps attribute.
    if not hasattr(env, "spec") or not hasattr(env.spec, "max_episode_steps"):
        if log_once("max_episode_steps"):
            logger.warning(
                "Your env doesn't have a .spec.max_episode_steps "
                "attribute. Your horizon will default "
                "to infinity, and your environment will not be "
                "reset."
            )

    # check if sampled actions and observations are contained within their
    # respective action and observation spaces.

    sampled_observation = env.observation_space.sample()
    sampled_action = env.action_space.sample()

    # Check, whether resetting works as expected.
    try:
        env.reset()
    except Exception as e:
        raise ValueError(
            "Your gymnasium.Env's `reset()` method raised an Exception!"
        ) from e

    # No more gym < 0.26 support! Error and explain the user how to upgrade to
    # gymnasium.
    try:
        # Important: Don't seed the env here by accident.
        # User would not notice and get stuck with an always fixed seeded env.
        obs_and_infos = env.reset(seed=None, options={})
        check_old_gym_env(reset_results=obs_and_infos)
    except Exception as e:
        raise ValueError(
            ERR_MSG_OLD_GYM_API.format(
                env, "In particular, the `reset()` method seems to be faulty."
            )
        ) from e
    reset_obs, reset_infos = obs_and_infos

    # Check if observation generated from resetting the environment is
    # contained within the observation space.
    if not env.observation_space.contains(reset_obs):
        temp_sampled_reset_obs = convert_element_to_space_type(
            reset_obs, sampled_observation
        )
        if not env.observation_space.contains(temp_sampled_reset_obs):
            # Find offending subspace in case we have a complex observation space.
            key, space, space_type, value, value_type = _find_offending_sub_space(
                env.observation_space, temp_sampled_reset_obs
            )
            raise ValueError(
                "The observation collected from env.reset() was not "
                "contained within your env's observation space. It is possible "
                "that there was a type mismatch, or that one of the "
                "sub-observations was out of bounds:\n {}(sub-)obs: {} ({})"
                "\n (sub-)observation space: {} ({})".format(
                    ("path: '" + key + "'\n ") if key else "",
                    value,
                    value_type,
                    space,
                    space_type,
                )
            )
    # sample a valid action in case of parametric actions
    if isinstance(reset_obs, dict):
        if config.action_mask_key in reset_obs:
            sampled_action = env.action_space.sample(
                mask=reset_obs[config.action_mask_key]
            )

    # Check if env.step can run, and generates observations rewards, done
    # signals and infos that are within their respective spaces and are of
    # the correct dtypes.
    try:
        results = env.step(sampled_action)
    except Exception as e:
        raise ValueError(
            "Your gymnasium.Env's `step()` method raised an Exception!"
        ) from e

    # No more gym < 0.26 support! Error and explain the user how to upgrade to
    # gymnasium.
    try:
        check_old_gym_env(step_results=results)
    except Exception as e:
        raise ValueError(
            ERR_MSG_OLD_GYM_API.format(
                env, "In particular, the `step()` method seems to be faulty."
            )
        ) from e
    next_obs, reward, done, truncated, info = results

    if not env.observation_space.contains(next_obs):
        temp_sampled_next_obs = convert_element_to_space_type(
            next_obs, sampled_observation
        )
        if not env.observation_space.contains(temp_sampled_next_obs):
            # Find offending subspace in case we have a complex observation space.
            key, space, space_type, value, value_type = _find_offending_sub_space(
                env.observation_space, temp_sampled_next_obs
            )
            error = (
                "The observation collected from env.step(sampled_action) was not "
                "contained within your env's observation space. It is possible "
                "that there was a type mismatch, or that one of the "
                "sub-observations was out of bounds: \n\n {}(sub-)obs: {} ({})"
                "\n (sub-)observation space: {} ({})".format(
                    ("path='" + key + "'\n ") if key else "",
                    value,
                    value_type,
                    space,
                    space_type,
                )
            )
            raise ValueError(error)
    _check_done_and_truncated(done, truncated)
    _check_reward(reward)
    _check_info(info)


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
        # No more gym < 0.26 support! Error and explain the user how to upgrade to
        # gymnasium.
        check_old_gym_env(reset_results=obs_and_infos)
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
        # No more gym < 0.26 support! Error and explain the user how to upgrade to
        # gymnasium.
        check_old_gym_env(step_results=results)
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
    _check_if_element_multi_agent_dict(env, info, "step, info")
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


@DeveloperAPI
def check_base_env(env: "BaseEnv") -> None:
    """Checking for common errors in RLlib BaseEnvs.

    Args:
        env: The env to be checked.
    """
    from ray.rllib.env import BaseEnv

    if not isinstance(env, BaseEnv):
        raise ValueError("The passed env is not a BaseEnv.")

    try:
        obs_and_infos = env.try_reset(seed=42, options={})
        # No more gym < 0.26 support! Error and explain the user how to upgrade to
        # gymnasium.
        check_old_gym_env(reset_results=obs_and_infos)
    except Exception as e:
        raise ValueError(
            ERR_MSG_OLD_GYM_API.format(
                env, "In particular, the `try_reset()` method seems to be faulty."
            )
        ) from e
    reset_obs, reset_infos = obs_and_infos

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

    try:
        results = env.poll()
        # No more gym < 0.26 support! Error and explain the user how to upgrade to
        # gymnasium.
        check_old_gym_env(step_results=results[:-1])
    except Exception as e:
        raise ValueError(
            ERR_MSG_OLD_GYM_API.format(
                env, "In particular, the `poll()` method seems to be faulty."
            )
        ) from e
    next_obs, reward, done, truncated, info, _ = results

    _check_if_multi_env_dict(env, next_obs, "step, next_obs")
    _check_if_multi_env_dict(env, reward, "step, reward")
    _check_if_multi_env_dict(env, done, "step, done")
    _check_if_multi_env_dict(env, truncated, "step, truncated")
    _check_if_multi_env_dict(env, info, "step, info")

    if not env.observation_space_contains(next_obs):
        error = (
            _not_contained_error("poll", "observation")
            + f": \n\n reset_obs: {reset_obs}\n\n env.step():{next_obs}\n\n"
        )
        raise ValueError(error)

    _check_reward(reward, base_env=True, agent_ids=env.get_agent_ids())
    _check_done_and_truncated(
        done,
        truncated,
        base_env=True,
        agent_ids=env.get_agent_ids(),
    )
    _check_info(info, base_env=True, agent_ids=env.get_agent_ids())


@DeveloperAPI
def check_vector_env(env: "VectorEnv") -> None:
    """Checking for common errors in RLlib VectorEnvs.

    Args:
        env: The env to be checked.
    """
    sampled_obs = env.observation_space.sample()

    # Test `vector_reset()`.
    try:
        vector_reset = env.vector_reset(
            seeds=[42] * env.num_envs,
            options=[{}] * env.num_envs,
        )
    except Exception as e:
        raise ValueError(
            "Your Env's `vector_reset()` method has some error! Make sure it expects a "
            "list of `seeds` (int) as well as a list of `options` dicts as optional, "
            "named args, e.g. def vector_reset(self, index: int, *, seeds: "
            "Optional[List[int]] = None, options: Optional[List[dict]] = None)"
        ) from e

    if not isinstance(vector_reset, tuple) or len(vector_reset) != 2:
        raise ValueError(
            "The `vector_reset()` method of your env must return a Tuple[obs, infos] as"
            f" of gym>=0.26! Your method returned: {vector_reset}."
        )
    reset_obs, reset_infos = vector_reset
    if not isinstance(reset_obs, list) or len(reset_obs) != env.num_envs:
        raise ValueError(
            "The observations returned by your env's `vector_reset()` method is NOT a "
            f"list or do not contain exactly `num_envs` ({env.num_envs}) items! "
            f"Your observations were: {reset_obs}"
        )
    if not isinstance(reset_infos, list) or len(reset_infos) != env.num_envs:
        raise ValueError(
            "The infos returned by your env's `vector_reset()` method is NOT a "
            f"list or do not contain exactly `num_envs` ({env.num_envs}) items! "
            f"Your infos were: {reset_infos}"
        )
    try:
        env.observation_space.contains(reset_obs[0])
    except Exception as e:
        raise ValueError(
            "Your `observation_space.contains` function has some error!"
        ) from e
    if not env.observation_space.contains(reset_obs[0]):
        error = (
            _not_contained_error("vector_reset", "observation")
            + f": \n\n reset_obs: {reset_obs}\n\n "
            f"env.observation_space.sample(): {sampled_obs}\n\n "
        )
        raise ValueError(error)

    # Test `reset_at()`.
    try:
        reset_at = env.reset_at(index=0, seed=42, options={})
    except Exception as e:
        raise ValueError(
            "Your Env's `reset_at()` method has some error! Make sure it expects a "
            "vector index (int) and an optional seed (int) as args."
        ) from e
    if not isinstance(reset_at, tuple) or len(reset_at) != 2:
        raise ValueError(
            "The `reset_at()` method of your env must return a Tuple[obs, infos] as "
            f"of gym>=0.26! Your method returned: {reset_at}."
        )
    reset_obs, reset_infos = reset_at
    if not isinstance(reset_infos, dict):
        raise ValueError(
            "The `reset_at()` method of your env must return an info dict as second "
            f"return value! Your method returned {reset_infos}"
        )
    if not env.observation_space.contains(reset_obs):
        error = (
            _not_contained_error("try_reset", "observation")
            + f": \n\n reset_obs: {reset_obs}\n\n "
            f"env.observation_space.sample(): {sampled_obs}\n\n "
        )
        raise ValueError(error)

    # Test `observation_space_sample()` and `observation_space_contains()`:
    if not env.observation_space.contains(sampled_obs):
        error = (
            _not_contained_error("observation_space.sample()", "observation")
            + f": \n\n sampled_obs: {sampled_obs}\n\n "
        )
        raise ValueError(error)

    # Test `vector_step()`
    sampled_action = env.action_space.sample()
    if not env.action_space.contains(sampled_action):
        error = (
            _not_contained_error("action_space.sample()", "action")
            + f": \n\n sampled_action {sampled_action}\n\n"
        )
        raise ValueError(error)

    step_results = env.vector_step([sampled_action for _ in range(env.num_envs)])
    if not isinstance(step_results, tuple) or len(step_results) != 5:
        raise ValueError(
            "The `vector_step()` method of your env must return a Tuple["
            "List[obs], List[rewards], List[terminateds], List[truncateds], "
            f"List[infos]] as of gym>=0.26! Your method returned: {step_results}."
        )

    obs, rewards, terminateds, truncateds, infos = step_results

    _check_if_vetor_env_list(env, obs, "step, obs")
    _check_if_vetor_env_list(env, rewards, "step, rewards")
    _check_if_vetor_env_list(env, terminateds, "step, terminateds")
    _check_if_vetor_env_list(env, truncateds, "step, truncateds")
    _check_if_vetor_env_list(env, infos, "step, infos")

    if not env.observation_space.contains(obs[0]):
        error = (
            _not_contained_error("vector_step", "observation")
            + f": \n\n obs: {obs[0]}\n\n env.vector_step():{obs}\n\n"
        )
        raise ValueError(error)

    _check_reward(rewards[0], base_env=False)
    _check_done_and_truncated(terminateds[0], truncateds[0], base_env=False)
    _check_info(infos[0], base_env=False)


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


def _check_if_vetor_env_list(env, element, function_string):
    if not isinstance(element, list) or len(element) != env.num_envs:
        raise ValueError(
            f"The element returned by {function_string} is not a "
            f"list OR the length of the returned list is not the same as the number of "
            f"sub-environments ({env.num_envs}) in your VectorEnv! "
            f"Instead, your {function_string} returned {element}"
        )


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

    Examples:
         >>> path, space, space_dtype, value, value_dtype = _find_offending_sub_space(
         ...     gym.spaces.Dict({
         ...    -2.0, 1.5, (2, ), np.int8), np.array([-1.5, 3.0])
         ... )
         >>> print(path)
         ...
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
