import logging
import os
import pprint
import random
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Tuple,
    Type,
)

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree
from gymnasium.spaces import (
    Box,
    Dict as GymDict,
    Discrete,
    MultiBinary,
    MultiDiscrete,
    Tuple as GymTuple,
)

import ray
from ray import tune
from ray._common.deprecation import Deprecated
from ray.rllib.core import DEFAULT_MODULE_ID, Columns
from ray.rllib.env.wrappers.atari_wrappers import is_atari, wrap_deepmind
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import try_import_jax, try_import_tf, try_import_torch
from ray.rllib.utils.metrics import (
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    ENV_RUNNER_RESULTS,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.typing import ResultDict
from ray.tune.result import TRAINING_ITERATION

if TYPE_CHECKING:
    from ray.rllib.algorithms import Algorithm, AlgorithmConfig
    from ray.rllib.offline.dataset_reader import DatasetReader

jax, _ = try_import_jax()
tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


@Deprecated(
    old="ray.rllib.utils.test_utils.add_rllib_example_script_args",
    new="ray.rllib.examples.utils.add_rllib_example_script_args",
    error=False,
)
def add_rllib_example_script_args(*args, **kwargs):
    from ray.rllib.examples.utils import add_rllib_example_script_args

    return add_rllib_example_script_args(*args, **kwargs)


@Deprecated(
    old="ray.rllib.utils.test_utils.should_stop",
    new="ray.rllib.examples.utils.should_stop",
    error=False,
)
def should_stop(*args, **kwargs):
    from ray.rllib.examples.utils import should_stop

    return should_stop(*args, **kwargs)


@Deprecated(
    old="ray.rllib.utils.test_utils.run_rllib_example_script_experiment",
    new="ray.rllib.examples.utils.run_rllib_example_script_experiment",
    error=False,
)
def run_rllib_example_script_experiment(*args, **kwargs):
    from ray.rllib.examples.utils import run_rllib_example_script_experiment

    return run_rllib_example_script_experiment(*args, **kwargs)


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
    # Nones or primitives (excluding int vs float, which should be compared with
    # tolerance/decimals as well).
    elif (
        x is None
        or y is None
        or isinstance(x, str)
        or (isinstance(x, int) and isinstance(y, int))
    ):
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
    # Also includes int vs float comparison, which is performed with tolerance/decimals.
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

        # Stats objects.
        from ray.rllib.utils.metrics.stats import StatsBase

        if isinstance(x, StatsBase):
            x = x.peek()
        if isinstance(y, StatsBase):
            y = y.peek()

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
    from ray.rllib.utils.metrics import (
        ENV_RUNNER_RESULTS,
        FAULT_TOLERANCE_STATS,
        LEARNER_RESULTS,
        TIMERS,
    )

    # Assert that some keys are where we would expect them.
    for key in [
        ENV_RUNNER_RESULTS,
        FAULT_TOLERANCE_STATS,
        LEARNER_RESULTS,
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
        .is_multi_agent
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
        .is_multi_agent
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

        print(
            f"Testing reproducibility of {algo_class.__name__}"
            f" with {num_workers} workers"
        )
        print("/// config")
        pprint.pprint(algo_config.to_dict())
        # test tune.Tuner().fit() reproducibility
        results1 = tune.Tuner(
            algo_class,
            param_space=algo_config.to_dict(),
            run_config=tune.RunConfig(stop=stop_dict, verbose=1),
        ).fit()
        results1 = results1.get_best_result().metrics

        results2 = tune.Tuner(
            algo_class,
            param_space=algo_config.to_dict(),
            run_config=tune.RunConfig(stop=stop_dict, verbose=1),
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

    path = "offline/tests/data/cartpole/large.json"
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

    def add(self, framework: str = "torch", obs=True, state=False) -> Any:
        """Builds a new Model for the given framework."""
        model = self.models[framework] = self.config.build(framework=framework)

        # Pass a B=1 observation through the model.
        inputs = np.full(
            [1] + ([1] if state else []) + list(self.config.input_dims),
            self.random_fill_input_value,
        )
        if obs:
            inputs = {Columns.OBS: inputs}
        if state:
            inputs[Columns.STATE_IN] = tree.map_structure(
                lambda s: np.zeros(shape=[1] + list(s)), state
            )
        if framework == "torch":
            from ray.rllib.utils.torch_utils import convert_to_torch_tensor

            inputs = convert_to_torch_tensor(inputs)
        # w/ old specs: inputs = model.input_specs.fill(self.random_fill_input_value)

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


def check_supported_spaces(
    alg: str,
    config: "AlgorithmConfig",
    train: bool = True,
    check_bounds: bool = False,
    frameworks: Optional[Tuple[str, ...]] = None,
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
    # Do these imports here because otherwise we have circular imports.
    from ray.rllib.examples.envs.classes.random_env import RandomEnv
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
                    assert isinstance(algo.get_policy().model, TorchVisionNet)
                # 1D input: Expect FCNet.
                elif o_name == "continuous":
                    assert isinstance(algo.get_policy().model, TorchFCNet)
                # Could be either one: ComplexNet (if disabled Preprocessor)
                # or FCNet (w/ Preprocessor).
                elif o_name == "vector2d":
                    assert isinstance(
                        algo.get_policy().model, (TorchComplexNet, TorchFCNet)
                    )
            if train:
                algo.train()
            algo.stop()
        print("Test: {}, ran in {}s".format(stat, time.time() - t0))

    if not frameworks:
        frameworks = ("tf2", "tf", "torch")

    _do_check_remote = ray.remote(_do_check)
    _do_check_remote = _do_check_remote.options(num_gpus=1 if use_gpu else 0)
    # Test all action spaces first.
    for a_name in action_spaces_to_test.keys():
        o_name = default_observation_space
        ray.get(_do_check_remote.remote(alg, config, a_name, o_name))

    # Now test all observation spaces.
    for o_name in observation_spaces_to_test.keys():
        a_name = default_action_space
        ray.get(_do_check_remote.remote(alg, config, a_name, o_name))
