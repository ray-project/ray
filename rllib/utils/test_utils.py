import argparse
from collections import Counter
import copy
import gymnasium as gym
from gymnasium.spaces import Box, Discrete, MultiDiscrete, MultiBinary
from gymnasium.spaces import Dict as GymDict
from gymnasium.spaces import Tuple as GymTuple
import inspect
import json
import logging
import numpy as np
import os
import pprint
import random
import re
import sys
import time
import tree  # pip install dm_tree
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
import yaml

import ray
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.air.integrations.wandb import WandbLoggerCallback
from ray.rllib.common import SupportedFileType
from ray.rllib.env.wrappers.atari_wrappers import is_atari, wrap_deepmind
from ray.rllib.train import load_experiments_from_file
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_jax, try_import_tf, try_import_torch
from ray.rllib.utils.metrics import (
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED_LIFETIME,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
    NUM_EPISODES_LIFETIME,
)
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.error import UnsupportedSpaceException


from ray.tune import CLIReporter, run_experiments


if TYPE_CHECKING:
    from ray.rllib.algorithms import Algorithm, AlgorithmConfig
    from ray.rllib.offline.dataset_reader import DatasetReader

jax, _ = try_import_jax()
tf1, tf, tfv = try_import_tf()
if tf1:
    eager_mode = None
    try:
        from tensorflow.python.eager.context import eager_mode
    except (ImportError, ModuleNotFoundError):
        pass

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


def add_rllib_example_script_args(
    parser: Optional[argparse.ArgumentParser] = None,
    default_reward: float = 100.0,
    default_iters: int = 200,
    default_timesteps: int = 100000,
) -> argparse.ArgumentParser:
    """Adds RLlib-typical (and common) examples scripts command line args to a parser.

    TODO (sven): This function should be used by most of our examples scripts, which
     already mostly have this logic in them (but written out).

    Args:
        parser: The parser to add the arguments to. If None, create a new one.
        default_reward: The default value for the --stop-reward option.
        default_iters: The default value for the --stop-iters option.
        default_timesteps: The default value for the --stop-timesteps option.

    Returns:
        The altered (or newly created) parser object.
    """
    if parser is None:
        parser = argparse.ArgumentParser()

    # Algo and Algo config options.
    parser.add_argument(
        "--algo", type=str, default="PPO", help="The RLlib-registered algorithm to use."
    )
    parser.add_argument(
        "--enable-new-api-stack",
        action="store_true",
        help="Whether to use the `enable_rl_module_and_learner` config setting.",
    )
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="torch",
        help="The DL framework specifier.",
    )
    parser.add_argument(
        "--env",
        type=str,
        default=None,
        help="The gym.Env identifier to run the experiment with.",
    )
    parser.add_argument(
        "--num-env-runners",
        type=int,
        default=None,
        help="The number of (remote) EnvRunners to use for the experiment.",
    )
    parser.add_argument(
        "--num-agents",
        type=int,
        default=0,
        help="If 0 (default), will run as single-agent. If > 0, will run as "
        "multi-agent with the environment simply cloned n times and each agent acting "
        "independently at every single timestep. The overall reward for this "
        "experiment is then the sum over all individual agents' rewards.",
    )

    # Evaluation options.
    parser.add_argument(
        "--evaluation-num-env-runners",
        type=int,
        default=0,
        help="The number of evaluation (remote) EnvRunners to use for the experiment.",
    )
    parser.add_argument(
        "--evaluation-interval",
        type=int,
        default=0,
        help="Every how many iterations to run one round of evaluation. "
        "Use 0 (default) to disable evaluation.",
    )
    parser.add_argument(
        "--evaluation-duration",
        type=lambda v: v if v == "auto" else int(v),
        default=10,
        help="The number of evaluation units to run each evaluation round. "
        "Use `--evaluation-duration-unit` to count either in 'episodes' "
        "or 'timesteps'. If 'auto', will run as many as possible during train pass ("
        "`--evaluation-parallel-to-training` must be set then).",
    )
    parser.add_argument(
        "--evaluation-duration-unit",
        type=str,
        default="episodes",
        choices=["episodes", "timesteps"],
        help="The evaluation duration unit to count by. One of 'episodes' or "
        "'timesteps'. This unit will be run `--evaluation-duration` times in each "
        "evaluation round. If `--evaluation-duration=auto`, this setting does not "
        "matter.",
    )
    parser.add_argument(
        "--evaluation-parallel-to-training",
        action="store_true",
        help="Whether to run evaluation parallel to training. This might help speed up "
        "your overall iteration time. Be aware that when using this option, your "
        "reported evaluation results are referring to one iteration before the current "
        "one.",
    )

    # RLlib logging options.
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="The output directory to write trajectories to, which are collected by "
        "the algo's EnvRunners.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,  # None -> use default
        choices=["INFO", "DEBUG", "WARN", "ERROR"],
        help="The log-level to be used by the RLlib logger.",
    )

    # tune.Tuner options.
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Whether to NOT use tune.Tuner(), but rather a simple for-loop calling "
        "`algo.train()` repeatedly until one of the stop criteria is met.",
    )
    parser.add_argument(
        "--num-samples",
        type=int,
        default=1,
        help="How many (tune.Tuner.fit()) experiments to execute - if possible in "
        "parallel.",
    )
    parser.add_argument(
        "--verbose",
        type=int,
        default=2,
        help="The verbosity level for the `tune.Tuner()` running the experiment.",
    )
    parser.add_argument(
        "--checkpoint-freq",
        type=int,
        default=0,
        help=(
            "The frequency (in training iterations) with which to create checkpoints. "
            "Note that if --wandb-key is provided, all checkpoints will "
            "automatically be uploaded to WandB."
        ),
    )
    parser.add_argument(
        "--checkpoint-at-end",
        action="store_true",
        help=(
            "Whether to create a checkpoint at the very end of the experiment. "
            "Note that if --wandb-key is provided, all checkpoints will "
            "automatically be uploaded to WandB."
        ),
    )

    # WandB logging options.
    parser.add_argument(
        "--wandb-key",
        type=str,
        default=None,
        help="The WandB API key to use for uploading results.",
    )
    parser.add_argument(
        "--wandb-project",
        type=str,
        default=None,
        help="The WandB project name to use.",
    )
    parser.add_argument(
        "--wandb-run-name",
        type=str,
        default=None,
        help="The WandB run name to use.",
    )

    # Experiment stopping and testing criteria.
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=default_reward,
        help="Reward at which the script should stop training.",
    )
    parser.add_argument(
        "--stop-iters",
        type=int,
        default=default_iters,
        help="The number of iterations to train.",
    )
    parser.add_argument(
        "--stop-timesteps",
        type=int,
        default=default_timesteps,
        help="The number of (environment sampling) timesteps to train.",
    )
    parser.add_argument(
        "--as-test",
        action="store_true",
        help="Whether this script should be run as a test. If set, --stop-reward must "
        "be achieved within --stop-timesteps AND --stop-iters, otherwise this "
        "script will throw an exception at the end.",
    )
    parser.add_argument(
        "--as-release-test",
        action="store_true",
        help="Whether this script should be run as a release test. If set, "
        "all that applies to the --as-test option is true, plus, a short JSON summary "
        "will be written into a results file whose location is given by the ENV "
        "variable `TEST_OUTPUT_JSON`.",
    )

    # Learner scaling options.
    # Old API stack: config.num_gpus.
    # New API stack: config.num_learners (w/ num_gpus_per_learner=1).
    parser.add_argument(
        "--num-gpus",
        type=int,
        default=0,
        help="The number of GPUs/Learners to use. If none or not enough GPUs "
        "are available, will still create `--num-gpus` Learners, but place them on one "
        "CPU each, instead.",
    )

    # Ray init options.
    parser.add_argument("--num-cpus", type=int, default=0)
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.",
    )
    return parser


def check(x, y, decimals=5, atol=None, rtol=None, false=False):
    """
    Checks two structures (dict, tuple, list,
    np.array, float, int, etc..) for (almost) numeric identity.
    All numbers in the two structures have to match up to `decimal` digits
    after the floating point. Uses assertions.

    Args:
        x: The value to be compared (to the expectation: `y`). This
            may be a Tensor.
        y: The expected value to be compared to `x`. This must not
            be a tf-Tensor, but may be a tf/torch-Tensor.
        decimals: The number of digits after the floating point up to
            which all numeric values have to match.
        atol: Absolute tolerance of the difference between x and y
            (overrides `decimals` if given).
        rtol: Relative tolerance of the difference between x and y
            (overrides `decimals` if given).
        false: Whether to check that x and y are NOT the same.
    """
    # A dict type.
    if isinstance(x, dict):
        assert isinstance(y, dict), "ERROR: If x is dict, y needs to be a dict as well!"
        y_keys = set(x.keys())
        for key, value in x.items():
            assert key in y, f"ERROR: y does not have x's key='{key}'! y={y}"
            check(value, y[key], decimals=decimals, atol=atol, rtol=rtol, false=false)
            y_keys.remove(key)
        assert not y_keys, "ERROR: y contains keys ({}) that are not in x! y={}".format(
            list(y_keys), y
        )
    # A tuple type.
    elif isinstance(x, (tuple, list)):
        assert isinstance(
            y, (tuple, list)
        ), "ERROR: If x is tuple/list, y needs to be a tuple/list as well!"
        assert len(y) == len(
            x
        ), "ERROR: y does not have the same length as x ({} vs {})!".format(
            len(y), len(x)
        )
        for i, value in enumerate(x):
            check(value, y[i], decimals=decimals, atol=atol, rtol=rtol, false=false)
    # Boolean comparison.
    elif isinstance(x, (np.bool_, bool)):
        if false is True:
            assert bool(x) is not bool(y), f"ERROR: x ({x}) is y ({y})!"
        else:
            assert bool(x) is bool(y), f"ERROR: x ({x}) is not y ({y})!"
    # Nones or primitives.
    elif x is None or y is None or isinstance(x, (str, int)):
        if false is True:
            assert x != y, f"ERROR: x ({x}) is the same as y ({y})!"
        else:
            assert x == y, f"ERROR: x ({x}) is not the same as y ({y})!"
    # String/byte comparisons.
    elif (
        hasattr(x, "dtype") and (x.dtype == object or str(x.dtype).startswith("<U"))
    ) or isinstance(x, bytes):
        try:
            np.testing.assert_array_equal(x, y)
            if false is True:
                assert False, f"ERROR: x ({x}) is the same as y ({y})!"
        except AssertionError as e:
            if false is False:
                raise e
    # Everything else (assume numeric or tf/torch.Tensor).
    else:
        if tf1 is not None:
            # y should never be a Tensor (y=expected value).
            if isinstance(y, (tf1.Tensor, tf1.Variable)):
                # In eager mode, numpyize tensors.
                if tf.executing_eagerly():
                    y = y.numpy()
                else:
                    raise ValueError(
                        "`y` (expected value) must not be a Tensor. "
                        "Use numpy.ndarray instead"
                    )
            if isinstance(x, (tf1.Tensor, tf1.Variable)):
                # In eager mode, numpyize tensors.
                if tf1.executing_eagerly():
                    x = x.numpy()
                # Otherwise, use a new tf-session.
                else:
                    with tf1.Session() as sess:
                        x = sess.run(x)
                        return check(
                            x, y, decimals=decimals, atol=atol, rtol=rtol, false=false
                        )
        if torch is not None:
            if isinstance(x, torch.Tensor):
                x = x.detach().cpu().numpy()
            if isinstance(y, torch.Tensor):
                y = y.detach().cpu().numpy()

        # Using decimals.
        if atol is None and rtol is None:
            # Assert equality of both values.
            try:
                np.testing.assert_almost_equal(x, y, decimal=decimals)
            # Both values are not equal.
            except AssertionError as e:
                # Raise error in normal case.
                if false is False:
                    raise e
            # Both values are equal.
            else:
                # If false is set -> raise error (not expected to be equal).
                if false is True:
                    assert False, f"ERROR: x ({x}) is the same as y ({y})!"

        # Using atol/rtol.
        else:
            # Provide defaults for either one of atol/rtol.
            if atol is None:
                atol = 0
            if rtol is None:
                rtol = 1e-7
            try:
                np.testing.assert_allclose(x, y, atol=atol, rtol=rtol)
            except AssertionError as e:
                if false is False:
                    raise e
            else:
                if false is True:
                    assert False, f"ERROR: x ({x}) is the same as y ({y})!"


def check_compute_single_action(
    algorithm, include_state=False, include_prev_action_reward=False
):
    """Tests different combinations of args for algorithm.compute_single_action.

    Args:
        algorithm: The Algorithm object to test.
        include_state: Whether to include the initial state of the Policy's
            Model in the `compute_single_action` call.
        include_prev_action_reward: Whether to include the prev-action and
            -reward in the `compute_single_action` call.

    Raises:
        ValueError: If anything unexpected happens.
    """
    # Have to import this here to avoid circular dependency.
    from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch

    # Some Algorithms may not abide to the standard API.
    pid = DEFAULT_POLICY_ID
    try:
        # Multi-agent: Pick any learnable policy (or DEFAULT_POLICY if it's the only
        # one).
        pid = next(iter(algorithm.env_runner.get_policies_to_train()))
        pol = algorithm.get_policy(pid)
    except AttributeError:
        pol = algorithm.policy
    # Get the policy's model.
    model = pol.model

    action_space = pol.action_space

    def _test(
        what, method_to_test, obs_space, full_fetch, explore, timestep, unsquash, clip
    ):
        call_kwargs = {}
        if what is algorithm:
            call_kwargs["full_fetch"] = full_fetch
            call_kwargs["policy_id"] = pid

        obs = obs_space.sample()
        if isinstance(obs_space, Box):
            obs = np.clip(obs, -1.0, 1.0)
        state_in = None
        if include_state:
            state_in = model.get_initial_state()
            if not state_in:
                state_in = []
                i = 0
                while f"state_in_{i}" in model.view_requirements:
                    state_in.append(
                        model.view_requirements[f"state_in_{i}"].space.sample()
                    )
                    i += 1
        action_in = action_space.sample() if include_prev_action_reward else None
        reward_in = 1.0 if include_prev_action_reward else None

        if method_to_test == "input_dict":
            assert what is pol

            input_dict = {SampleBatch.OBS: obs}
            if include_prev_action_reward:
                input_dict[SampleBatch.PREV_ACTIONS] = action_in
                input_dict[SampleBatch.PREV_REWARDS] = reward_in
            if state_in:
                if what.config.get("enable_rl_module_and_learner", False):
                    input_dict["state_in"] = state_in
                else:
                    for i, s in enumerate(state_in):
                        input_dict[f"state_in_{i}"] = s
            input_dict_batched = SampleBatch(
                tree.map_structure(lambda s: np.expand_dims(s, 0), input_dict)
            )
            action = pol.compute_actions_from_input_dict(
                input_dict=input_dict_batched,
                explore=explore,
                timestep=timestep,
                **call_kwargs,
            )
            # Unbatch everything to be able to compare against single
            # action below.
            # ARS and ES return action batches as lists.
            if isinstance(action[0], list):
                action = (np.array(action[0]), action[1], action[2])
            action = tree.map_structure(lambda s: s[0], action)

            try:
                action2 = pol.compute_single_action(
                    input_dict=input_dict,
                    explore=explore,
                    timestep=timestep,
                    **call_kwargs,
                )
                # Make sure these are the same, unless we have exploration
                # switched on (or noisy layers).
                if not explore and not pol.config.get("noisy"):
                    check(action, action2)
            except TypeError:
                pass
        else:
            action = what.compute_single_action(
                obs,
                state_in,
                prev_action=action_in,
                prev_reward=reward_in,
                explore=explore,
                timestep=timestep,
                unsquash_action=unsquash,
                clip_action=clip,
                **call_kwargs,
            )

        state_out = None
        if state_in or full_fetch or what is pol:
            action, state_out, _ = action
        if state_out:
            for si, so in zip(tree.flatten(state_in), tree.flatten(state_out)):
                if tf.is_tensor(si):
                    # If si is a tensor of Dimensions, we need to convert it
                    # We expect this to be the case for TF RLModules who's initial
                    # states are Tf Tensors.
                    si_shape = si.shape.as_list()
                else:
                    si_shape = list(si.shape)
                check(si_shape, so.shape)

        if unsquash is None:
            unsquash = what.config["normalize_actions"]
        if clip is None:
            clip = what.config["clip_actions"]

        # Test whether unsquash/clipping works on the Algorithm's
        # compute_single_action method: Both flags should force the action
        # to be within the space's bounds.
        if method_to_test == "single" and what == algorithm:
            if not action_space.contains(action) and (
                clip or unsquash or not isinstance(action_space, Box)
            ):
                raise ValueError(
                    f"Returned action ({action}) of algorithm/policy {what} "
                    f"not in Env's action_space {action_space}"
                )
            # We are operating in normalized space: Expect only smaller action
            # values.
            if (
                isinstance(action_space, Box)
                and not unsquash
                and what.config.get("normalize_actions")
                and np.any(np.abs(action) > 15.0)
            ):
                raise ValueError(
                    f"Returned action ({action}) of algorithm/policy {what} "
                    "should be in normalized space, but seems too large/small "
                    "for that!"
                )

    # Loop through: Policy vs Algorithm; Different API methods to calculate
    # actions; unsquash option; clip option; full fetch or not.
    for what in [pol, algorithm]:
        if what is algorithm:
            # Get the obs-space from Workers.env (not Policy) due to possible
            # pre-processor up front.
            worker_set = getattr(algorithm, "env_runner_group", None)
            assert worker_set
            if not worker_set.local_env_runner:
                obs_space = algorithm.get_policy(pid).observation_space
            else:
                obs_space = worker_set.local_env_runner.for_policy(
                    lambda p: p.observation_space, policy_id=pid
                )
            obs_space = getattr(obs_space, "original_space", obs_space)
        else:
            obs_space = pol.observation_space

        for method_to_test in ["single"] + (["input_dict"] if what is pol else []):
            for explore in [True, False]:
                for full_fetch in [False, True] if what is algorithm else [False]:
                    timestep = random.randint(0, 100000)
                    for unsquash in [True, False, None]:
                        for clip in [False] if unsquash else [True, False, None]:
                            print("-" * 80)
                            print(f"what={what}")
                            print(f"method_to_test={method_to_test}")
                            print(f"explore={explore}")
                            print(f"full_fetch={full_fetch}")
                            print(f"unsquash={unsquash}")
                            print(f"clip={clip}")
                            _test(
                                what,
                                method_to_test,
                                obs_space,
                                full_fetch,
                                explore,
                                timestep,
                                unsquash,
                                clip,
                            )


def check_inference_w_connectors(policy, env_name, max_steps: int = 100):
    """Checks whether the given policy can infer actions from an env with connectors.

    Args:
        policy: The policy to check.
        env_name: Name of the environment to check
        max_steps: The maximum number of steps to run the environment for.

    Raises:
        ValueError: If the policy cannot infer actions from the environment.
    """
    # Avoids circular import
    from ray.rllib.utils.policy import local_policy_inference

    env = gym.make(env_name)

    # Potentially wrap the env like we do in RolloutWorker
    if is_atari(env):
        env = wrap_deepmind(
            env,
            dim=policy.config["model"]["dim"],
            framestack=policy.config["model"].get("framestack"),
        )

    obs, info = env.reset()
    reward, terminated, truncated = 0.0, False, False
    ts = 0
    while not terminated and not truncated and ts < max_steps:
        action_out = local_policy_inference(
            policy,
            env_id=0,
            agent_id=0,
            obs=obs,
            reward=reward,
            terminated=terminated,
            truncated=truncated,
            info=info,
        )
        obs, reward, terminated, truncated, info = env.step(action_out[0][0])

        ts += 1


def check_learning_achieved(
    tune_results: "tune.ResultGrid",
    min_value: float,
    evaluation: Optional[bool] = None,
    metric: str = f"{ENV_RUNNER_RESULTS}/episode_return_mean",
):
    """Throws an error if `min_reward` is not reached within tune_results.

    Checks the last iteration found in tune_results for its
    "episode_return_mean" value and compares it to `min_reward`.

    Args:
        tune_results: The tune.Tuner().fit() returned results object.
        min_reward: The min reward that must be reached.
        evaluation: If True, use `evaluation/env_runners/[metric]`, if False, use
            `env_runners/[metric]`, if None, use evaluation sampler results if
            available otherwise, use train sampler results.

    Raises:
        ValueError: If `min_reward` not reached.
    """
    # Get maximum value of `metrics` over all trials
    # (check if at least one trial achieved some learning, not just the final one).
    recorded_values = []
    for _, row in tune_results.get_dataframe().iterrows():
        if evaluation or (
            evaluation is None and f"{EVALUATION_RESULTS}/{metric}" in row
        ):
            recorded_values.append(row[f"{EVALUATION_RESULTS}/{metric}"])
        else:
            recorded_values.append(row[metric])
    best_value = max(recorded_values)
    if best_value < min_value:
        raise ValueError(f"`{metric}` of {min_value} not reached!")
    print(f"`{metric}` of {min_value} reached! ok")


def check_off_policyness(
    results: ResultDict,
    upper_limit: float,
    lower_limit: float = 0.0,
) -> Optional[float]:
    """Verifies that the off-policy'ness of some update is within some range.

    Off-policy'ness is defined as the average (across n workers) diff
    between the number of gradient updates performed on the policy used
    for sampling vs the number of gradient updates that have been performed
    on the trained policy (usually the one on the local worker).

    Uses the published DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY metric inside
    a training results dict and compares to the given bounds.

    Note: Only works with single-agent results thus far.

    Args:
        results: The training results dict.
        upper_limit: The upper limit to for the off_policy_ness value.
        lower_limit: The lower limit to for the off_policy_ness value.

    Returns:
        The off-policy'ness value (described above).

    Raises:
        AssertionError: If the value is out of bounds.
    """

    # Have to import this here to avoid circular dependency.
    from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
    from ray.rllib.utils.metrics.learner_info import LEARNER_INFO

    # Assert that the off-policy'ness is within the given bounds.
    learner_info = results["info"][LEARNER_INFO]
    if DEFAULT_POLICY_ID not in learner_info:
        return None
    off_policy_ness = learner_info[DEFAULT_POLICY_ID][
        DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY
    ]
    # Roughly: Reaches up to 0.4 for 2 rollout workers and up to 0.2 for
    # 1 rollout worker.
    if not (lower_limit <= off_policy_ness <= upper_limit):
        raise AssertionError(
            f"`off_policy_ness` ({off_policy_ness}) is outside the given bounds "
            f"({lower_limit} - {upper_limit})!"
        )

    return off_policy_ness


def check_train_results_new_api_stack(train_results: ResultDict) -> None:
    """Checks proper structure of a Algorithm.train() returned dict.

    Args:
        train_results: The train results dict to check.

    Raises:
        AssertionError: If `train_results` doesn't have the proper structure or
            data in it.
    """
    # Import these here to avoid circular dependencies.
    from ray.rllib.core import DEFAULT_MODULE_ID
    from ray.rllib.utils.metrics import (
        ENV_RUNNER_RESULTS,
        FAULT_TOLERANCE_STATS,
        LEARNER_RESULTS,
        NUM_AGENT_STEPS_SAMPLED_LIFETIME,
        NUM_ENV_STEPS_SAMPLED_LIFETIME,
        TIMERS,
    )

    # Assert that some keys are where we would expect them.
    for key in [
        ENV_RUNNER_RESULTS,
        FAULT_TOLERANCE_STATS,
        LEARNER_RESULTS,
        NUM_AGENT_STEPS_SAMPLED_LIFETIME,
        NUM_ENV_STEPS_SAMPLED_LIFETIME,
        TIMERS,
        TRAINING_ITERATION,
        "config",
    ]:
        assert (
            key in train_results
        ), f"'{key}' not found in `train_results` ({train_results})!"

    # Make sure, `config` is an actual dict, not an AlgorithmConfig object.
    assert isinstance(
        train_results["config"], dict
    ), "`config` in results not a python dict!"

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    is_multi_agent = (
        AlgorithmConfig()
        .update_from_dict({"policies": train_results["config"]["policies"]})
        .is_multi_agent()
    )

    # Check in particular the "info" dict.
    learner_results = train_results[LEARNER_RESULTS]

    # Make sure we have a `DEFAULT_MODULE_ID key if we are not in a
    # multi-agent setup.
    if not is_multi_agent:
        assert len(learner_results) == 0 or DEFAULT_MODULE_ID in learner_results, (
            f"'{DEFAULT_MODULE_ID}' not found in "
            f"train_results['{LEARNER_RESULTS}']!"
        )

    for module_id, module_metrics in learner_results.items():
        # The ModuleID can be __all_modules__ in multi-agent case when the new learner
        # stack is enabled.
        if module_id == "__all_modules__":
            continue

        # On the new API stack, policy has no LEARNER_STATS_KEY under it anymore.
        for key, value in module_metrics.items():
            # Min- and max-stats should be single values.
            if key.endswith("_min") or key.endswith("_max"):
                assert np.isscalar(value), f"'key' value not a scalar ({value})!"

    return train_results


@OldAPIStack
def check_train_results(train_results: ResultDict):
    """Checks proper structure of a Algorithm.train() returned dict.

    Args:
        train_results: The train results dict to check.

    Raises:
        AssertionError: If `train_results` doesn't have the proper structure or
            data in it.
    """
    # Import these here to avoid circular dependencies.
    from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
    from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY

    # Assert that some keys are where we would expect them.
    for key in [
        "config",
        "custom_metrics",
        ENV_RUNNER_RESULTS,
        "info",
        "iterations_since_restore",
        "num_healthy_workers",
        "perf",
        "time_since_restore",
        "time_this_iter_s",
        "timers",
        "time_total_s",
        TRAINING_ITERATION,
    ]:
        assert (
            key in train_results
        ), f"'{key}' not found in `train_results` ({train_results})!"

    for key in [
        "episode_len_mean",
        "episode_reward_max",
        "episode_reward_mean",
        "episode_reward_min",
        "hist_stats",
        "policy_reward_max",
        "policy_reward_mean",
        "policy_reward_min",
        "sampler_perf",
    ]:
        assert key in train_results[ENV_RUNNER_RESULTS], (
            f"'{key}' not found in `train_results[ENV_RUNNER_RESULTS]` "
            f"({train_results[ENV_RUNNER_RESULTS]})!"
        )

    # Make sure, `config` is an actual dict, not an AlgorithmConfig object.
    assert isinstance(
        train_results["config"], dict
    ), "`config` in results not a python dict!"

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    is_multi_agent = (
        AlgorithmConfig()
        .update_from_dict({"policies": train_results["config"]["policies"]})
        .is_multi_agent()
    )

    # Check in particular the "info" dict.
    info = train_results["info"]
    assert LEARNER_INFO in info, f"'learner' not in train_results['infos'] ({info})!"
    assert (
        "num_steps_trained" in info or NUM_ENV_STEPS_TRAINED in info
    ), f"'num_(env_)?steps_trained' not in train_results['infos'] ({info})!"

    learner_info = info[LEARNER_INFO]

    # Make sure we have a default_policy key if we are not in a
    # multi-agent setup.
    if not is_multi_agent:
        # APEX algos sometimes have an empty learner info dict (no metrics
        # collected yet).
        assert len(learner_info) == 0 or DEFAULT_POLICY_ID in learner_info, (
            f"'{DEFAULT_POLICY_ID}' not found in "
            f"train_results['infos']['learner'] ({learner_info})!"
        )

    for pid, policy_stats in learner_info.items():
        if pid == "batch_count":
            continue

        # the pid can be __all__ in multi-agent case when the new learner stack is
        # enabled.
        if pid == "__all__":
            continue

        # On the new API stack, policy has no LEARNER_STATS_KEY under it anymore.
        if LEARNER_STATS_KEY in policy_stats:
            learner_stats = policy_stats[LEARNER_STATS_KEY]
        else:
            learner_stats = policy_stats
        for key, value in learner_stats.items():
            # Min- and max-stats should be single values.
            if key.startswith("min_") or key.startswith("max_"):
                assert np.isscalar(value), f"'key' value not a scalar ({value})!"

    return train_results


def framework_iterator(
    config: Optional["AlgorithmConfig"] = None,
    frameworks: Sequence[str] = ("tf2", "tf", "torch"),
    session: bool = False,
    time_iterations: Optional[dict] = None,
) -> Union[str, Tuple[str, Optional["tf1.Session"]]]:
    """An generator that allows for looping through n frameworks for testing.

    Provides the correct config entries ("framework") as well
    as the correct eager/non-eager contexts for tf/tf2.

    Args:
        config: An optional config dict or AlgorithmConfig object. This will be modified
            (value for "framework" changed) depending on the iteration.
        frameworks: A list/tuple of the frameworks to be tested.
            Allowed are: "tf2", "tf", "torch", and None.
        session: If True and only in the tf-case: Enter a tf.Session()
            and yield that as second return value (otherwise yield (fw, None)).
            Also sets a seed (42) on the session to make the test
            deterministic.
        time_iterations: If provided, will write to the given dict (by
            framework key) the times in seconds that each (framework's)
            iteration takes.

    Yields:
        If `session` is False: The current framework [tf2|tf|torch] used.
        If `session` is True: A tuple consisting of the current framework
        string and the tf1.Session (if fw="tf", otherwise None).
    """
    config = config or {}
    frameworks = [frameworks] if isinstance(frameworks, str) else list(frameworks)

    for fw in frameworks:
        # Skip tf if on new API stack.
        if fw == "tf" and config.get("enable_rl_module_and_learner", False):
            logger.warning("Skipping `framework=tf` (new API stack configured)!")
            continue
        # Skip if tf/tf2 and py >= 3.11.
        elif fw in ["tf", "tf2"] and (
            sys.version_info.major == 3 and sys.version_info.minor >= 9
        ):
            logger.warning("Skipping `framework=tf/tf2` (python >= 3.9)!")
            continue

        # Skip non-installed frameworks.
        if fw == "torch" and not torch:
            logger.warning("framework_iterator skipping torch (not installed)!")
            continue
        if fw != "torch" and not tf:
            logger.warning(
                "framework_iterator skipping {} (tf not installed)!".format(fw)
            )
            continue
        elif fw == "tf2" and tfv != 2:
            logger.warning("framework_iterator skipping tf2.x (tf version is < 2.0)!")
            continue
        elif fw == "jax" and not jax:
            logger.warning("framework_iterator skipping JAX (not installed)!")
            continue
        assert fw in ["tf2", "tf", "torch", "jax", None]

        # Do we need a test session?
        sess = None
        if fw == "tf" and session is True:
            sess = tf1.Session()
            sess.__enter__()
            tf1.set_random_seed(42)

        if isinstance(config, dict):
            config["framework"] = fw
        else:
            config.framework(fw)

        eager_ctx = None
        # Enable eager mode for tf2.
        if fw == "tf2":
            eager_ctx = eager_mode()
            eager_ctx.__enter__()
            assert tf1.executing_eagerly()
        # Make sure, eager mode is off.
        elif fw == "tf":
            assert not tf1.executing_eagerly()

        # Yield current framework + tf-session (if necessary).
        print(f"framework={fw}")
        time_started = time.time()
        yield fw if session is False else (fw, sess)
        if time_iterations is not None:
            time_total = time.time() - time_started
            time_iterations[fw] = time_total
            print(f".. took {time_total}sec")

        # Exit any context we may have entered.
        if eager_ctx:
            eager_ctx.__exit__(None, None, None)
        elif sess:
            sess.__exit__(None, None, None)


@Deprecated(new="run_learning_tests_from_yaml_or_py(config_files=...)", error=False)
def run_learning_tests_from_yaml(
    yaml_files: List[str],
    *,
    framework: Optional[str] = None,
    max_num_repeats: int = 2,
    use_pass_criteria_as_stop: bool = True,
    smoke_test: bool = False,
):
    return run_learning_tests_from_yaml_or_py(
        yaml_files,
        framework=framework,
        max_num_repeats=max_num_repeats,
        use_pass_criteria_as_stop=use_pass_criteria_as_stop,
        smoke_test=smoke_test,
    )


def run_learning_tests_from_yaml_or_py(
    config_files: List[str],
    *,
    framework: Optional[str] = None,
    max_num_repeats: int = 2,
    use_pass_criteria_as_stop: bool = True,
    smoke_test: bool = False,
) -> Dict[str, Any]:
    """Runs the given experiments in config_files and returns results dict.

    Args:
        framework: The framework to use for running this test. If None,
            run the test on all frameworks.
        config_files: List of yaml or py config file names.
        max_num_repeats: How many times should we repeat a failed
            experiment?
        use_pass_criteria_as_stop: Configure the Trial so that it stops
            as soon as pass criterias are met.
        smoke_test: Whether this is just a smoke-test. If True,
            set time_total_s to 5min and don't early out due to rewards
            or timesteps reached.

    Returns:
        A results dict mapping strings (e.g. "time_taken", "stats", "passed") to
            the respective stats/values.
    """
    print("Will run the following config files:")
    for config_file in config_files:
        print("->", config_file)

    # All trials we'll ever run in this test script.
    all_trials = []
    # The experiments (by name) we'll run up to `max_num_repeats` times.
    experiments = {}
    # The results per experiment.
    checks = {}
    # Metrics per experiment.
    stats = {}

    start_time = time.monotonic()

    def should_check_eval(experiment):
        # If we have evaluation workers, use their rewards.
        # This is useful for offline learning tests, where
        # we evaluate against an actual environment.
        return bool(experiment["config"].get("evaluation_interval"))

    # Loop through all collected files and gather experiments.
    # Set correct framework(s).
    for config_file in config_files:
        # For python files, need to make sure, we only deliver the module name into the
        # `load_experiments_from_file` function (everything from "/ray/rllib" on).
        if config_file.endswith(".py"):
            if config_file.endswith(
                "__init__.py"
            ):  # weird CI learning test (BAZEL) case
                continue
            tf_experiments = load_experiments_from_file(
                config_file, SupportedFileType.python
            )
        else:
            tf_experiments = load_experiments_from_file(
                config_file, SupportedFileType.yaml
            )

        # Add torch version of all experiments to the list.
        for k, e in tf_experiments.items():
            # If framework given as arg, use that framework.
            if framework is not None:
                frameworks = [framework]
            # If framework given in config, only test for that framework.
            # Some algos do not have both versions available.
            elif "frameworks" in e:
                frameworks = e["frameworks"]
            else:
                # By default we don't run tf2, because tf2's multi-gpu support
                # isn't complete yet.
                frameworks = ["tf", "torch"]
            # Pop frameworks key to not confuse Tune.
            e.pop("frameworks", None)

            e["stop"] = e["stop"] if "stop" in e else {}
            e["pass_criteria"] = e["pass_criteria"] if "pass_criteria" in e else {}

            check_eval = should_check_eval(e)
            episode_reward_key = (
                f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
                if not check_eval
                else f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
            )

            # For smoke-tests, we just run for n min.
            if smoke_test:
                # 0sec for each(!) experiment/trial.
                # This is such that if there are many experiments/trials
                # in a test (e.g. rllib_learning_test), each one can at least
                # create its Algorithm and run a first iteration.
                e["stop"]["time_total_s"] = 0
            else:
                if use_pass_criteria_as_stop:
                    # We also stop early, once we reach the desired reward.
                    min_reward = e.get("pass_criteria", {}).get(episode_reward_key)
                    if min_reward is not None:
                        e["stop"][episode_reward_key] = min_reward

            # Generate `checks` dict for all experiments
            # (tf, tf2 and/or torch).
            for framework in frameworks:
                k_ = k + "-" + framework
                ec = copy.deepcopy(e)
                ec["config"]["framework"] = framework
                if framework == "tf2":
                    ec["config"]["eager_tracing"] = True

                checks[k_] = {
                    "min_reward": ec["pass_criteria"].get(episode_reward_key, 0.0),
                    "min_throughput": ec["pass_criteria"].get("timesteps_total", 0.0)
                    / (ec["stop"].get("time_total_s", 1.0) or 1.0),
                    "time_total_s": ec["stop"].get("time_total_s"),
                    "failures": 0,
                    "passed": False,
                }
                # This key would break tune.
                ec.pop("pass_criteria", None)

                # One experiment to run.
                experiments[k_] = ec

    # Keep track of those experiments we still have to run.
    # If an experiment passes, we'll remove it from this dict.
    experiments_to_run = experiments.copy()

    # When running as a release test, use `/mnt/cluster_storage` as the storage path.
    release_test_storage_path = "/mnt/cluster_storage"
    if os.path.exists(release_test_storage_path):
        for k, e in experiments_to_run.items():
            e["storage_path"] = release_test_storage_path

    try:
        ray.init(address="auto")
    except ConnectionError:
        ray.init()

    for i in range(max_num_repeats):
        # We are done.
        if len(experiments_to_run) == 0:
            print("All experiments finished.")
            break

        print(f"Starting learning test iteration {i}...")

        # Print out the actual config.
        print("== Test config ==")
        print(yaml.dump(experiments_to_run))

        # Run remaining experiments.
        trials = run_experiments(
            experiments_to_run,
            resume=False,
            verbose=2,
            progress_reporter=CLIReporter(
                metric_columns={
                    TRAINING_ITERATION: "iter",
                    "time_total_s": "time_total_s",
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: "ts (sampled)",
                    NUM_ENV_STEPS_TRAINED_LIFETIME: "ts (trained)",
                    NUM_EPISODES_LIFETIME: "train_episodes",
                    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": "reward_mean",
                    (
                        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/"
                        f"{EPISODE_RETURN_MEAN}"
                    ): "eval_reward_mean",
                },
                parameter_columns=["framework"],
                sort_by_metric=True,
                max_report_frequency=30,
            ),
        )

        all_trials.extend(trials)

        # Check each experiment for whether it passed.
        # Criteria is to a) reach reward AND b) to have reached the throughput
        # defined by `NUM_ENV_STEPS_(SAMPLED|TRAINED)` / `time_total_s`.
        for experiment in experiments_to_run.copy():
            print(f"Analyzing experiment {experiment} ...")
            # Collect all trials within this experiment (some experiments may
            # have num_samples or grid_searches defined).
            trials_for_experiment = []
            for t in trials:
                trial_exp = re.sub(".+/([^/]+)$", "\\1", t.local_dir)
                if trial_exp == experiment:
                    trials_for_experiment.append(t)
            print(f" ... Trials: {trials_for_experiment}.")

            check_eval = should_check_eval(experiments[experiment])

            # Error: Increase failure count and repeat.
            if any(t.status == "ERROR" for t in trials_for_experiment):
                print(" ... ERROR.")
                checks[experiment]["failures"] += 1
            # Smoke-tests always succeed.
            elif smoke_test:
                print(" ... SMOKE TEST (mark ok).")
                checks[experiment]["passed"] = True
                del experiments_to_run[experiment]
            # Experiment finished: Check reward achieved and timesteps done
            # (throughput).
            else:
                # Use best_result's reward to check min_reward.
                if check_eval:
                    episode_return_mean = np.mean(
                        [
                            t.metric_analysis[
                                f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/"
                                f"{EPISODE_RETURN_MEAN}"
                            ]["max"]
                            for t in trials_for_experiment
                        ]
                    )
                else:
                    episode_return_mean = np.mean(
                        [
                            t.metric_analysis[
                                f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
                            ]["max"]
                            for t in trials_for_experiment
                        ]
                    )
                desired_reward = checks[experiment]["min_reward"]

                # Use last_result["timesteps_total"] to check throughput.
                timesteps_total = np.mean(
                    [t.last_result["timesteps_total"] for t in trials_for_experiment]
                )
                total_time_s = np.mean(
                    [t.last_result["time_total_s"] for t in trials_for_experiment]
                )

                # TODO(jungong) : track training- and env throughput separately.
                throughput = timesteps_total / (total_time_s or 1.0)
                # Throughput verification is not working. Many algorithm, e.g. TD3,
                # achieves the learning goal, but fails the throughput check
                # miserably.
                # TODO(jungong): Figure out why.
                #
                # desired_throughput = checks[experiment]["min_throughput"]
                desired_throughput = None

                # Record performance.
                stats[experiment] = {
                    "episode_reward_mean": float(episode_return_mean),
                    "throughput": (
                        float(throughput) if throughput is not None else 0.0
                    ),
                }

                print(
                    f" ... Desired reward={desired_reward}; "
                    f"desired throughput={desired_throughput}"
                )

                # We failed to reach desired reward or the desired throughput.
                if (desired_reward and episode_return_mean < desired_reward) or (
                    desired_throughput and throughput < desired_throughput
                ):
                    print(
                        " ... Not successful: Actual "
                        f"return={episode_return_mean}; "
                        f"actual throughput={throughput}"
                    )
                    checks[experiment]["failures"] += 1
                # We succeeded!
                else:
                    print(
                        " ... Successful: (mark ok). Actual "
                        f"return={episode_return_mean}; "
                        f"actual throughput={throughput}"
                    )
                    checks[experiment]["passed"] = True
                    del experiments_to_run[experiment]

    ray.shutdown()

    time_taken = time.monotonic() - start_time

    # Create results dict and write it to disk.
    result = {
        "time_taken": float(time_taken),
        "trial_states": dict(Counter([trial.status for trial in all_trials])),
        "last_update": float(time.time()),
        "stats": stats,
        "passed": [k for k, exp in checks.items() if exp["passed"]],
        "not_passed": [k for k, exp in checks.items() if not exp["passed"]],
        "failures": {
            k: exp["failures"] for k, exp in checks.items() if exp["failures"] > 0
        },
    }

    return result


# TODO (sven): Make this the de-facto, well documented, and unified utility for most of
#  our tests:
#  - CI (label: "learning_tests")
#  - release tests (benchmarks)
#  - example scripts
def run_rllib_example_script_experiment(
    base_config: "AlgorithmConfig",
    args: Optional[argparse.Namespace] = None,
    *,
    stop: Optional[Dict] = None,
    success_metric: Optional[Dict] = None,
    trainable: Optional[Type] = None,
    tune_callbacks: Optional[List] = None,
    keep_config: bool = False,
    scheduler=None,
    progress_reporter=None,
) -> Union[ResultDict, tune.result_grid.ResultGrid]:
    """Given an algorithm config and some command line args, runs an experiment.

    There are some constraints on what properties must be defined in `args`.
    It should ideally be generated via calling
    `args = add_rllib_example_script_args()`, which can be found in this very module
    here.

    The function sets up an Algorithm object from the given config (altered by the
    contents of `args`), then runs the Algorithm via Tune (or manually, if
    `args.no_tune` is set to True) using the stopping criteria in `stop`.

    At the end of the experiment, if `args.as_test` is True, checks, whether the
    Algorithm reached the `success_metric` (if None, use `env_runners/
    episode_return_mean` with a minimum value of `args.stop_reward`).

    See https://github.com/ray-project/ray/tree/master/rllib/examples for an overview
    of all supported command line options.

    Args:
        base_config: The AlgorithmConfig object to use for this experiment. This base
            config will be automatically "extended" based on some of the provided
            `args`. For example, `args.num_env_runners` is used to set
            `config.num_env_runners`, etc..
        args: A argparse.Namespace object, ideally returned by calling
            `args = add_rllib_example_script_args()`. It must have the following
            properties defined: `stop_iters`, `stop_reward`, `stop_timesteps`,
            `no_tune`, `verbose`, `checkpoint_freq`, `as_test`. Optionally, for WandB
            logging: `wandb_key`, `wandb_project`, `wandb_run_name`.
        stop: An optional dict mapping ResultDict key strings (using "/" in case of
            nesting, e.g. "env_runners/episode_return_mean" for referring to
            `result_dict['env_runners']['episode_return_mean']` to minimum
            values, reaching of which will stop the experiment). Default is:
            {
            "env_runners/episode_return_mean": args.stop_reward,
            "training_iteration": args.stop_iters,
            "num_env_steps_sampled_lifetime": args.stop_timesteps,
            }
        success_metric: Only relevant if `args.as_test` is True.
            A dict mapping a single(!) ResultDict key string (using "/" in
            case of nesting, e.g. "env_runners/episode_return_mean" for referring
            to `result_dict['env_runners']['episode_return_mean']` to a single(!)
            minimum value to be reached in order for the experiment to count as
            successful. If `args.as_test` is True AND this `success_metric` is not
            reached with the bounds defined by `stop`, will raise an Exception.
        trainable: The Trainable sub-class to run in the tune.Tuner. If None (default),
            use the registered RLlib Algorithm class specified by args.algo.
        tune_callbacks: A list of Tune callbacks to configure with the tune.Tuner.
            In case `args.wandb_key` is provided, appends a WandB logger to this
            list.
        keep_config: Set this to True, if you don't want this utility to change the
            given `base_config` in any way and leave it as-is. This is helpful
            for those example scripts which demonstrate how to set config settings
            that are taken care of automatically in this function otherwise (e.g.
            `num_env_runners`).

    Returns:
        The last ResultDict from a --no-tune run OR the tune.Tuner.fit()
        results.
    """
    if args is None:
        parser = add_rllib_example_script_args()
        args = parser.parse_args()

    # If run --as-release-test, --as-test must also be set.
    if args.as_release_test:
        args.as_test = True

    # Initialize Ray.
    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    # Define one or more stopping criteria.
    if stop is None:
        stop = {
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
            f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": args.stop_timesteps,
            TRAINING_ITERATION: args.stop_iters,
        }

    config = base_config

    # Enhance the `base_config`, based on provided `args`.
    if not keep_config:
        # Set the framework.
        config.framework(args.framework)

        # Add an env specifier (only if not already set in config)?
        if args.env is not None and config.env is None:
            config.environment(args.env)

        # Enable the new API stack?
        if args.enable_new_api_stack:
            config.api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )

        # Define EnvRunner/RolloutWorker scaling and behavior.
        if args.num_env_runners is not None:
            config.env_runners(num_env_runners=args.num_env_runners)

        # Define compute resources used automatically (only using the --num-gpus arg).
        # New stack.
        if config.enable_rl_module_and_learner:
            # Define compute resources used.
            config.resources(num_gpus=0)
            config.learners(
                num_learners=args.num_gpus,
                num_gpus_per_learner=(
                    1
                    if torch and torch.cuda.is_available() and args.num_gpus > 0
                    else 0
                ),
            )
            config.resources(num_gpus=0)
        # Old stack.
        else:
            config.resources(num_gpus=args.num_gpus)

        # Evaluation setup.
        if args.evaluation_interval > 0:
            config.evaluation(
                evaluation_num_env_runners=args.evaluation_num_env_runners,
                evaluation_interval=args.evaluation_interval,
                evaluation_duration=args.evaluation_duration,
                evaluation_duration_unit=args.evaluation_duration_unit,
                evaluation_parallel_to_training=args.evaluation_parallel_to_training,
            )

        # Set the log-level (if applicable).
        if args.log_level is not None:
            config.debugging(log_level=args.log_level)

        # Set the output dir (if applicable).
        if args.output is not None:
            config.offline_data(output=args.output)

    # Run the experiment w/o Tune (directly operate on the RLlib Algorithm object).
    if args.no_tune:
        assert not args.as_test and not args.as_release_test
        algo = config.build()
        for i in range(stop.get(TRAINING_ITERATION, args.stop_iters)):
            results = algo.train()
            if ENV_RUNNER_RESULTS in results:
                print(
                    f"iter={i} R={results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]}",
                    end="",
                )
            if EVALUATION_RESULTS in results:
                Reval = results[EVALUATION_RESULTS][ENV_RUNNER_RESULTS][
                    EPISODE_RETURN_MEAN
                ]
                print(f" R(eval)={Reval}", end="")
            print()
            for key, threshold in stop.items():
                val = results
                for k in key.split("/"):
                    try:
                        val = val[k]
                    except KeyError:
                        val = None
                        break
                if val is not None and not np.isnan(val) and val >= threshold:
                    print(f"Stop criterium ({key}={threshold}) fulfilled!")
                    ray.shutdown()
                    return results

        ray.shutdown()
        return results

    # Run the experiment using Ray Tune.

    # Log results using WandB.
    tune_callbacks = tune_callbacks or []
    if hasattr(args, "wandb_key") and args.wandb_key is not None:
        project = args.wandb_project or (
            args.algo.lower() + "-" + re.sub("\\W+", "-", str(config.env).lower())
        )
        tune_callbacks.append(
            WandbLoggerCallback(
                api_key=args.wandb_key,
                project=project,
                upload_checkpoints=True,
                **({"name": args.wandb_run_name} if args.wandb_run_name else {}),
            )
        )

    # Auto-configure a CLIReporter (to log the results to the console).
    # Use better ProgressReporter for multi-agent cases: List individual policy rewards.
    if progress_reporter is None and args.num_agents > 0:
        progress_reporter = CLIReporter(
            metric_columns={
                **{
                    TRAINING_ITERATION: "iter",
                    "time_total_s": "total time (s)",
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: "ts",
                    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": "combined return",
                },
                **{
                    (
                        f"{ENV_RUNNER_RESULTS}/module_episode_returns_mean/" f"{pid}"
                    ): f"return {pid}"
                    for pid in config.policies
                },
            },
        )

    # Force Tuner to use old progress output as the new one silently ignores our custom
    # `CLIReporter`.
    os.environ["RAY_AIR_NEW_OUTPUT"] = "0"

    # Run the actual experiment (using Tune).
    start_time = time.time()
    results = tune.Tuner(
        trainable or config.algo_class,
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=args.verbose,
            callbacks=tune_callbacks,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=args.checkpoint_freq,
                checkpoint_at_end=args.checkpoint_at_end,
            ),
            progress_reporter=progress_reporter,
        ),
        tune_config=tune.TuneConfig(
            num_samples=args.num_samples,
            scheduler=scheduler,
        ),
    ).fit()
    time_taken = time.time() - start_time

    ray.shutdown()

    # If run as a test, check whether we reached the specified success criteria.
    test_passed = False
    if args.as_test:
        # Success metric not provided, try extracting it from `stop`.
        if success_metric is None:
            for try_it in [
                f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
                f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
            ]:
                if try_it in stop:
                    success_metric = {try_it: stop[try_it]}
                    break
            if success_metric is None:
                success_metric = {
                    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
                }
        # TODO (sven): Make this work for more than one metric (AND-logic?).
        # Get maximum value of `metric` over all trials
        # (check if at least one trial achieved some learning, not just the final one).
        success_metric_key, success_metric_value = next(iter(success_metric.items()))
        best_value = max(
            row[success_metric_key] for _, row in results.get_dataframe().iterrows()
        )
        if best_value >= success_metric_value:
            test_passed = True
            print(f"`{success_metric_key}` of {success_metric_value} reached! ok")

        if args.as_release_test:
            trial = results._experiment_analysis.trials[0]
            stats = trial.last_result
            stats.pop("config", None)
            json_summary = {
                "time_taken": float(time_taken),
                "trial_states": [trial.status],
                "last_update": float(time.time()),
                "stats": stats,
                "passed": [test_passed],
                "not_passed": [not test_passed],
                "failures": {str(trial): 1} if not test_passed else {},
            }
            with open(
                os.environ.get("TEST_OUTPUT_JSON", "/tmp/learning_test.json"),
                "wt",
            ) as f:
                try:
                    json.dump(json_summary, f)
                # Something went wrong writing json. Try again w/ simplified stats.
                except Exception:
                    from ray.rllib.algorithms.algorithm import Algorithm

                    simplified_stats = {
                        k: stats[k] for k in Algorithm._progress_metrics if k in stats
                    }
                    json_summary["stats"] = simplified_stats
                    json.dump(json_summary, f)

        if not test_passed:
            raise ValueError(
                f"`{success_metric_key}` of {success_metric_value} not reached!"
            )

    return results


def check_same_batch(batch1, batch2) -> None:
    """Check if both batches are (almost) identical.

    For MultiAgentBatches, the step count and individual policy's
    SampleBatches are checked for identity. For SampleBatches, identity is
    checked as the almost numerical key-value-pair identity between batches
    with ray.rllib.utils.test_utils.check(). unroll_id is compared only if
    both batches have an unroll_id.

    Args:
        batch1: Batch to compare against batch2
        batch2: Batch to compare against batch1
    """
    # Avoids circular import
    from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch

    assert type(batch1) is type(
        batch2
    ), "Input batches are of different types {} and {}".format(
        str(type(batch1)), str(type(batch2))
    )

    def check_sample_batches(_batch1, _batch2, _policy_id=None):
        unroll_id_1 = _batch1.get("unroll_id", None)
        unroll_id_2 = _batch2.get("unroll_id", None)
        # unroll IDs only have to fit if both batches have them
        if unroll_id_1 is not None and unroll_id_2 is not None:
            assert unroll_id_1 == unroll_id_2

        batch1_keys = set()
        for k, v in _batch1.items():
            # unroll_id is compared above already
            if k == "unroll_id":
                continue
            check(v, _batch2[k])
            batch1_keys.add(k)

        batch2_keys = set(_batch2.keys())
        # unroll_id is compared above already
        batch2_keys.discard("unroll_id")
        _difference = batch1_keys.symmetric_difference(batch2_keys)

        # Cases where one batch has info and the other has not
        if _policy_id:
            assert not _difference, (
                "SampleBatches for policy with ID {} "
                "don't share information on the "
                "following information: \n{}"
                "".format(_policy_id, _difference)
            )
        else:
            assert not _difference, (
                "SampleBatches don't share information "
                "on the following information: \n{}"
                "".format(_difference)
            )

    if type(batch1) is SampleBatch:
        check_sample_batches(batch1, batch2)
    elif type(batch1) is MultiAgentBatch:
        assert batch1.count == batch2.count
        batch1_ids = set()
        for policy_id, policy_batch in batch1.policy_batches.items():
            check_sample_batches(
                policy_batch, batch2.policy_batches[policy_id], policy_id
            )
            batch1_ids.add(policy_id)

        # Case where one ma batch has info on a policy the other has not
        batch2_ids = set(batch2.policy_batches.keys())
        difference = batch1_ids.symmetric_difference(batch2_ids)
        assert (
            not difference
        ), f"MultiAgentBatches don't share the following information: \n{difference}."
    else:
        raise ValueError("Unsupported batch type " + str(type(batch1)))


def check_reproducibilty(
    algo_class: Type["Algorithm"],
    algo_config: "AlgorithmConfig",
    *,
    fw_kwargs: Dict[str, Any],
    training_iteration: int = 1,
) -> None:
    # TODO @kourosh: we can get rid of examples/deterministic_training.py once
    # this is added to all algorithms
    """Check if the algorithm is reproducible across different testing conditions:

        frameworks: all input frameworks
        num_gpus: int(os.environ.get("RLLIB_NUM_GPUS", "0"))
        num_workers: 0 (only local workers) or
                     4 ((1) local workers + (4) remote workers)
        num_envs_per_env_runner: 2

    Args:
        algo_class: Algorithm class to test.
        algo_config: Base config to use for the algorithm.
        fw_kwargs: Framework iterator keyword arguments.
        training_iteration: Number of training iterations to run.

    Returns:
        None

    Raises:
        It raises an AssertionError if the algorithm is not reproducible.
    """
    from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
    from ray.rllib.utils.metrics.learner_info import LEARNER_INFO

    stop_dict = {TRAINING_ITERATION: training_iteration}
    # use 0 and 2 workers (for more that 4 workers we have to make sure the instance
    # type in ci build has enough resources)
    for num_workers in [0, 2]:
        algo_config = (
            algo_config.debugging(seed=42).env_runners(
                num_env_runners=num_workers, num_envs_per_env_runner=2
            )
            # new API
            .learners(
                num_gpus_per_learner=int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            )
            # old API
            .resources(
                num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            )
        )

        for fw in framework_iterator(algo_config, **fw_kwargs):
            print(
                f"Testing reproducibility of {algo_class.__name__}"
                f" with {num_workers} workers on fw = {fw}"
            )
            print("/// config")
            pprint.pprint(algo_config.to_dict())
            # test tune.Tuner().fit() reproducibility
            results1 = tune.Tuner(
                algo_class,
                param_space=algo_config.to_dict(),
                run_config=air.RunConfig(stop=stop_dict, verbose=1),
            ).fit()
            results1 = results1.get_best_result().metrics

            results2 = tune.Tuner(
                algo_class,
                param_space=algo_config.to_dict(),
                run_config=air.RunConfig(stop=stop_dict, verbose=1),
            ).fit()
            results2 = results2.get_best_result().metrics

            # Test rollout behavior.
            check(
                results1[ENV_RUNNER_RESULTS]["hist_stats"],
                results2[ENV_RUNNER_RESULTS]["hist_stats"],
            )
            # As well as training behavior (minibatch sequence during SGD
            # iterations).
            # As well as training behavior (minibatch sequence during SGD
            # iterations).
            if algo_config.enable_rl_module_and_learner:
                check(
                    results1["info"][LEARNER_INFO][DEFAULT_POLICY_ID],
                    results2["info"][LEARNER_INFO][DEFAULT_POLICY_ID],
                )
            else:
                check(
                    results1["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
                    results2["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
                )


def get_cartpole_dataset_reader(batch_size: int = 1) -> "DatasetReader":
    """Returns a DatasetReader for the cartpole dataset.
    Args:
        batch_size: The batch size to use for the reader.
    Returns:
        A rllib DatasetReader for the cartpole dataset.
    """
    from ray.rllib.algorithms import AlgorithmConfig
    from ray.rllib.offline import IOContext
    from ray.rllib.offline.dataset_reader import (
        DatasetReader,
        get_dataset_and_shards,
    )

    path = "tests/data/cartpole/large.json"
    input_config = {"format": "json", "paths": path}
    dataset, _ = get_dataset_and_shards(
        AlgorithmConfig().offline_data(input_="dataset", input_config=input_config)
    )
    ioctx = IOContext(
        config=(
            AlgorithmConfig()
            .training(train_batch_size=batch_size)
            .offline_data(actions_in_input_normalized=True)
        ),
        worker_index=0,
    )
    reader = DatasetReader(dataset, ioctx)
    return reader


class ModelChecker:
    """Helper class to compare architecturally identical Models across frameworks.

    Holds a ModelConfig, such that individual models can be added simply via their
    framework string (by building them with config.build(framework=...).
    A call to `check()` forces all added models to be compared in terms of their
    number of trainable and non-trainable parameters, as well as, their
    computation results given a common weights structure and values and identical
    inputs to the models.
    """

    def __init__(self, config):
        self.config = config

        # To compare number of params between frameworks.
        self.param_counts = {}
        # To compare computed outputs from fixed-weights-nets between frameworks.
        self.output_values = {}

        # We will pass an observation filled with this one random value through
        # all DL networks (after they have been set to fixed-weights) to compare
        # the computed outputs.
        self.random_fill_input_value = np.random.uniform(-0.01, 0.01)

        # Dict of models to check against each other.
        self.models = {}

    def add(self, framework: str = "torch") -> Any:
        """Builds a new Model for the given framework."""
        model = self.models[framework] = self.config.build(framework=framework)

        # Pass a B=1 observation through the model.
        from ray.rllib.core.models.specs.specs_dict import SpecDict

        if isinstance(model.input_specs, SpecDict):
            # inputs = {}

            def _fill(s):
                if s is not None:
                    return s.fill(self.random_fill_input_value)
                else:
                    return None

            inputs = tree.map_structure(_fill, dict(model.input_specs))
            # for key, spec in model.input_specs.items():
            #    dict_ = inputs
            #    for i, sub_key in enumerate(key):
            #        if sub_key not in dict_:
            #            dict_[sub_key] = {}
            #        if i < len(key) - 1:
            #            dict_ = dict_[sub_key]
            #    if spec is not None:
            #        dict_[sub_key] = spec.fill(self.random_fill_input_value)
            #    else:
            #        dict_[sub_key] = None
        else:
            inputs = model.input_specs.fill(self.random_fill_input_value)

        outputs = model(inputs)

        # Bring model into a reproducible, comparable state (so we can compare
        # computations across frameworks). Use only a value-sequence of len=1 here
        # as it could possibly be that the layers are stored in different order
        # across the different frameworks.
        model._set_to_dummy_weights(value_sequence=(self.random_fill_input_value,))

        # Perform another forward pass.
        comparable_outputs = model(inputs)

        # Store the number of parameters for this framework's net.
        self.param_counts[framework] = model.get_num_parameters()
        # Store the fixed-weights-net outputs for this framework's net.
        if framework == "torch":
            self.output_values[framework] = tree.map_structure(
                lambda s: s.detach().numpy() if s is not None else None,
                comparable_outputs,
            )
        else:
            self.output_values[framework] = tree.map_structure(
                lambda s: s.numpy() if s is not None else None, comparable_outputs
            )
        return outputs

    def check(self):
        """Compares all added Models with each other and possibly raises errors."""

        main_key = next(iter(self.models.keys()))
        # Compare number of trainable and non-trainable params between all
        # frameworks.
        for c in self.param_counts.values():
            check(c, self.param_counts[main_key])

        # Compare dummy outputs by exact values given that all nets received the
        # same input and all nets have the same (dummy) weight values.
        for v in self.output_values.values():
            check(v, self.output_values[main_key], atol=0.0005)


def _get_mean_action_from_algorithm(alg: "Algorithm", obs: np.ndarray) -> np.ndarray:
    """Returns the mean action computed by the given algorithm.

    Note: This makes calls to `Algorithm.compute_single_action`

    Args:
        alg: The constructed algorithm to run inference on.
        obs: The observation to compute the action for.

    Returns:
        The mean action computed by the algorithm over 5000 samples.

    """
    out = []
    for _ in range(5000):
        out.append(float(alg.compute_single_action(obs)))
    return np.mean(out)


def test_ckpt_restore(
    config: "AlgorithmConfig",
    env_name: str,
    tf2=False,
    replay_buffer=False,
    run_restored_algorithm=True,
    eval_env_runner_group=False,
):
    """Test that after an algorithm is trained, its checkpoint can be restored.

    Check the replay buffers of the algorithm to see if they have identical data.
    Check the optimizer weights of the policy on the algorithm to see if they're
    identical.

    Args:
        config: The config of the algorithm to be trained.
        env_name: The name of the gymansium environment to be trained on.
        tf2: Whether to test the algorithm with the tf2 framework or not.
        object_store: Whether to test checkpointing with objects from the object store.
        replay_buffer: Whether to test checkpointing with replay buffers.
        run_restored_algorithm: Whether to run the restored algorithm after restoring.
        eval_env_runner_group: Whether to also inspect the eval EnvRunnerGroup of the
            Algorithm.

    """
    # config = algorithms_and_configs[algo_name].to_dict()
    # If required, store replay buffer data in checkpoints as well.
    if replay_buffer:
        config["store_buffer_in_checkpoints"] = True

    frameworks = (["tf2"] if tf2 else []) + ["torch", "tf"]
    for fw in framework_iterator(config, frameworks=frameworks):
        env = gym.make(env_name)
        alg1 = config.environment(env_name).framework(fw).build()
        alg2 = config.environment(env_name).build()

        policy1 = alg1.get_policy()

        res = alg1.train()
        print("current status: " + str(res))

        # Check optimizer state as well.
        optim_state = policy1.get_state().get("_optimizer_variables")

        checkpoint = alg1.save()

        # Test if we can restore multiple times (at least twice, assuming failure
        # would mainly stem from improperly reused variables)
        for num_restores in range(2):
            # Sync the models
            alg2.restore(checkpoint)

        # Compare optimizer state with re-loaded one.
        if optim_state:
            s2 = alg2.get_policy().get_state().get("_optimizer_variables")
            # Tf -> Compare states 1:1.
            if fw in ["tf2", "tf"]:
                check(s2, optim_state)
            # For torch, optimizers have state_dicts with keys=params,
            # which are different for the two models (ignore these
            # different keys, but compare all values nevertheless).
            else:
                for i, s2_ in enumerate(s2):
                    check(
                        list(s2_["state"].values()),
                        list(optim_state[i]["state"].values()),
                    )

        # Compare buffer content with restored one.
        if replay_buffer:
            data = alg1.local_replay_buffer.replay_buffers["default_policy"]._storage[
                42 : 42 + 42
            ]
            new_data = alg2.local_replay_buffer.replay_buffers[
                "default_policy"
            ]._storage[42 : 42 + 42]
            check(data, new_data)

        # Check, whether the eval EnvRunnerGroup has the same policies and
        # `policy_mapping_fn`.
        if eval_env_runner_group:
            eval_mapping_src = inspect.getsource(alg1.eval_env_runner.policy_mapping_fn)
            check(
                eval_mapping_src,
                inspect.getsource(alg2.eval_env_runner.policy_mapping_fn),
            )
            check(
                eval_mapping_src,
                inspect.getsource(alg2.env_runner.policy_mapping_fn),
                false=True,
            )

        for _ in range(1):
            obs = env.observation_space.sample()
            a1 = _get_mean_action_from_algorithm(alg1, obs)
            a2 = _get_mean_action_from_algorithm(alg2, obs)
            print("Checking computed actions", alg1, obs, a1, a2)
            if abs(a1 - a2) > 0.1:
                raise AssertionError(
                    "algo={} [a1={} a2={}]".format(str(alg1.__class__), a1, a2)
                )
        # Stop algo 1.
        alg1.stop()

        if run_restored_algorithm:
            # Check that algo 2 can still run.
            print("Starting second run on Algo 2...")
            alg2.train()
        alg2.stop()


def check_supported_spaces(
    alg: str,
    config: "AlgorithmConfig",
    train: bool = True,
    check_bounds: bool = False,
    frameworks: Optional[Tuple[str]] = None,
    use_gpu: bool = False,
):
    """Checks whether the given algorithm supports different action and obs spaces.

        Performs the checks by constructing an rllib algorithm from the config and
        checking to see that the model inside the policy is the correct one given
        the action and obs spaces. For example if the action space is discrete and
        the obs space is an image, then the model should be a vision network with
        a categorical action distribution.

    Args:
        alg: The name of the algorithm to test.
        config: The config to use for the algorithm.
        train: Whether to train the algorithm for a few iterations.
        check_bounds: Whether to check the bounds of the action space.
        frameworks: The frameworks to test the algorithm with.
        use_gpu: Whether to check support for training on a gpu.


    """
    # do these imports here because otherwise we have circular imports
    from ray.rllib.examples.envs.classes.random_env import RandomEnv
    from ray.rllib.models.tf.complex_input_net import ComplexInputNetwork as ComplexNet
    from ray.rllib.models.tf.fcnet import FullyConnectedNetwork as FCNet
    from ray.rllib.models.tf.visionnet import VisionNetwork as VisionNet
    from ray.rllib.models.torch.complex_input_net import (
        ComplexInputNetwork as TorchComplexNet,
    )
    from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFCNet
    from ray.rllib.models.torch.visionnet import VisionNetwork as TorchVisionNet

    action_spaces_to_test = {
        # Test discrete twice here until we support multi_binary action spaces
        "discrete": Discrete(5),
        "continuous": Box(-1.0, 1.0, (5,), dtype=np.float32),
        "int_actions": Box(0, 3, (2, 3), dtype=np.int32),
        "multidiscrete": MultiDiscrete([1, 2, 3, 4]),
        "tuple": GymTuple(
            [Discrete(2), Discrete(3), Box(-1.0, 1.0, (5,), dtype=np.float32)]
        ),
        "dict": GymDict(
            {
                "action_choice": Discrete(3),
                "parameters": Box(-1.0, 1.0, (1,), dtype=np.float32),
                "yet_another_nested_dict": GymDict(
                    {"a": GymTuple([Discrete(2), Discrete(3)])}
                ),
            }
        ),
    }

    observation_spaces_to_test = {
        "multi_binary": MultiBinary([3, 10, 10]),
        "discrete": Discrete(5),
        "continuous": Box(-1.0, 1.0, (5,), dtype=np.float32),
        "vector2d": Box(-1.0, 1.0, (5, 5), dtype=np.float32),
        "image": Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32),
        "tuple": GymTuple([Discrete(10), Box(-1.0, 1.0, (5,), dtype=np.float32)]),
        "dict": GymDict(
            {
                "task": Discrete(10),
                "position": Box(-1.0, 1.0, (5,), dtype=np.float32),
            }
        ),
    }

    # The observation spaces that we test RLModules with
    rlmodule_supported_observation_spaces = [
        "multi_binary",
        "discrete",
        "continuous",
        "image",
        "tuple",
        "dict",
    ]

    rlmodule_supported_frameworks = ("torch", "tf2")

    # The action spaces that we test RLModules with
    rlmodule_supported_action_spaces = ["discrete", "continuous"]

    default_observation_space = default_action_space = "discrete"

    config["log_level"] = "ERROR"
    config["env"] = RandomEnv

    def _do_check(alg, config, a_name, o_name):
        # We need to copy here so that this validation does not affect the actual
        # validation method call further down the line.
        config_copy = config.copy()
        config_copy.validate()
        # If RLModules are enabled, we need to skip a few tests for now:
        if config_copy.enable_rl_module_and_learner:
            # Skip PPO cases in which RLModules don't support the given spaces yet.
            if o_name not in rlmodule_supported_observation_spaces:
                logger.warning(
                    "Skipping PPO test with RLModules for obs space {}".format(o_name)
                )
                return
            if a_name not in rlmodule_supported_action_spaces:
                logger.warning(
                    "Skipping PPO test with RLModules for action space {}".format(
                        a_name
                    )
                )
                return

        fw = config["framework"]
        action_space = action_spaces_to_test[a_name]
        obs_space = observation_spaces_to_test[o_name]
        print(
            "=== Testing {} (fw={}) action_space={} obs_space={} ===".format(
                alg, fw, action_space, obs_space
            )
        )
        t0 = time.time()
        config.update_from_dict(
            dict(
                env_config=dict(
                    action_space=action_space,
                    observation_space=obs_space,
                    reward_space=Box(1.0, 1.0, shape=(), dtype=np.float32),
                    p_terminated=1.0,
                    check_action_bounds=check_bounds,
                )
            )
        )
        stat = "ok"

        try:
            algo = config.build()
        except ray.exceptions.RayActorError as e:
            if len(e.args) >= 2 and isinstance(e.args[2], UnsupportedSpaceException):
                stat = "unsupported"
            elif isinstance(e.args[0].args[2], UnsupportedSpaceException):
                stat = "unsupported"
            else:
                raise
        except UnsupportedSpaceException:
            stat = "unsupported"
        else:
            if alg not in ["SAC", "PPO"]:
                # 2D (image) input: Expect VisionNet.
                if o_name in ["atari", "image"]:
                    if fw == "torch":
                        assert isinstance(algo.get_policy().model, TorchVisionNet)
                    else:
                        assert isinstance(algo.get_policy().model, VisionNet)
                # 1D input: Expect FCNet.
                elif o_name == "continuous":
                    if fw == "torch":
                        assert isinstance(algo.get_policy().model, TorchFCNet)
                    else:
                        assert isinstance(algo.get_policy().model, FCNet)
                # Could be either one: ComplexNet (if disabled Preprocessor)
                # or FCNet (w/ Preprocessor).
                elif o_name == "vector2d":
                    if fw == "torch":
                        assert isinstance(
                            algo.get_policy().model, (TorchComplexNet, TorchFCNet)
                        )
                    else:
                        assert isinstance(algo.get_policy().model, (ComplexNet, FCNet))
            if train:
                algo.train()
            algo.stop()
        print("Test: {}, ran in {}s".format(stat, time.time() - t0))

    if not frameworks:
        frameworks = ("tf2", "tf", "torch")

    if config.enable_rl_module_and_learner:
        # Only test the frameworks that are supported by RLModules.
        frameworks = tuple(
            fw for fw in frameworks if fw in rlmodule_supported_frameworks
        )

    _do_check_remote = ray.remote(_do_check)
    _do_check_remote = _do_check_remote.options(num_gpus=1 if use_gpu else 0)
    for _ in framework_iterator(config, frameworks=frameworks):
        # Test all action spaces first.
        for a_name in action_spaces_to_test.keys():
            o_name = default_observation_space
            ray.get(_do_check_remote.remote(alg, config, a_name, o_name))

        # Now test all observation spaces.
        for o_name in observation_spaces_to_test.keys():
            a_name = default_action_space
            ray.get(_do_check_remote.remote(alg, config, a_name, o_name))
