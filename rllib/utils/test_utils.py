from collections import Counter
import copy
import gymnasium as gym
from gymnasium.spaces import Box
import logging
import numpy as np
import os
import pprint
import random
import re
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
from ray.rllib.utils.framework import try_import_jax, try_import_tf, try_import_torch
from ray.rllib.env.wrappers.atari_wrappers import is_atari, wrap_deepmind
from ray.rllib.utils.metrics import (
    DIFF_NUM_GRAD_UPDATES_VS_SAMPLER_POLICY,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.typing import PartialAlgorithmConfigDict, ResultDict
from ray.tune import CLIReporter, run_experiments


if TYPE_CHECKING:
    from ray.rllib.algorithms import Algorithm, AlgorithmConfig

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


def framework_iterator(
    config: Optional["AlgorithmConfig"] = None,
    frameworks: Sequence[str] = ("tf2", "tf", "torch"),
    session: bool = False,
    with_eager_tracing: bool = False,
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
        with_eager_tracing: Include `eager_tracing=True` in the returned
            configs, when framework=tf2.
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

        # Additionally loop through eager_tracing=True + False, if necessary.
        if fw == "tf2" and with_eager_tracing:
            for tracing in [True, False]:
                if isinstance(config, dict):
                    config["eager_tracing"] = tracing
                else:
                    config.framework(eager_tracing=tracing)
                print(f"framework={fw} (eager-tracing={tracing})")
                time_started = time.time()
                yield fw if session is False else (fw, sess)
                if time_iterations is not None:
                    time_total = time.time() - time_started
                    time_iterations[fw + ("+tracing" if tracing else "")] = time_total
                    print(f".. took {time_total}sec")
                if isinstance(config, dict):
                    config["eager_tracing"] = False
                else:
                    config.framework(eager_tracing=False)
        # Yield current framework + tf-session (if necessary).
        else:
            print(f"framework={fw}")
            time_started = time.time()
            yield fw if session is False else (fw, sess)
            if time_iterations is not None:
                time_total = time.time() - time_started
                time_iterations[fw + ("+tracing" if tracing else "")] = time_total
                print(f".. took {time_total}sec")

        # Exit any context we may have entered.
        if eager_ctx:
            eager_ctx.__exit__(None, None, None)
        elif sess:
            sess.__exit__(None, None, None)


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
        ), "ERROR: If x is tuple, y needs to be a tuple as well!"
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
        pid = next(iter(algorithm.workers.local_worker().get_policies_to_train()))
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
            for si, so in zip(state_in, state_out):
                check(list(si.shape), so.shape)

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
            worker_set = getattr(algorithm, "workers", None)
            assert worker_set
            if not worker_set.local_worker():
                obs_space = algorithm.get_policy(pid).observation_space
            else:
                obs_space = worker_set.local_worker().for_policy(
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

    # TODO(sven): Remove this if-block once gymnasium fully supports Atari envs.
    if env_name.startswith("ALE/"):
        env = gym.make("GymV26Environment-v0", env_id=env_name)
    else:
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
    tune_results: "tune.ResultGrid", min_reward, evaluation=False
):
    """Throws an error if `min_reward` is not reached within tune_results.

    Checks the last iteration found in tune_results for its
    "episode_reward_mean" value and compares it to `min_reward`.

    Args:
        tune_results: The tune.Tuner().fit() returned results object.
        min_reward: The min reward that must be reached.

    Raises:
        ValueError: If `min_reward` not reached.
    """
    # Get maximum reward of all trials
    # (check if at least one trial achieved some learning)
    avg_rewards = [
        (
            row["episode_reward_mean"]
            if not evaluation
            else row["evaluation/episode_reward_mean"]
        )
        for _, row in tune_results.get_dataframe().iterrows()
    ]
    best_avg_reward = max(avg_rewards)
    if best_avg_reward < min_reward:
        raise ValueError(f"`stop-reward` of {min_reward} not reached!")
    print(f"`stop-reward` of {min_reward} reached! ok")


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


def check_train_results(train_results: PartialAlgorithmConfigDict) -> ResultDict:
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
        "agent_timesteps_total",
        "config",
        "custom_metrics",
        "episode_len_mean",
        "episode_reward_max",
        "episode_reward_mean",
        "episode_reward_min",
        "episodes_total",
        "hist_stats",
        "info",
        "iterations_since_restore",
        "num_healthy_workers",
        "perf",
        "policy_reward_max",
        "policy_reward_mean",
        "policy_reward_min",
        "sampler_perf",
        "time_since_restore",
        "time_this_iter_s",
        "timesteps_since_restore",
        "timesteps_total",
        "timers",
        "time_total_s",
        "training_iteration",
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
        .update_from_dict(train_results["config"]["multiagent"])
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

        # Make sure each policy has the LEARNER_STATS_KEY under it.
        assert LEARNER_STATS_KEY in policy_stats
        learner_stats = policy_stats[LEARNER_STATS_KEY]
        for key, value in learner_stats.items():
            # Min- and max-stats should be single values.
            if key.startswith("min_") or key.startswith("max_"):
                assert np.isscalar(value), f"'key' value not a scalar ({value})!"

    return train_results


def run_learning_tests_from_yaml(
    yaml_files: List[str],
    *,
    framework: Optional[str] = None,
    max_num_repeats: int = 2,
    use_pass_criteria_as_stop: bool = True,
    smoke_test: bool = False,
) -> Dict[str, Any]:
    """Runs the given experiments in yaml_files and returns results dict.

    Args:
        framework: The framework to use for running this test. If None,
            run the test on all frameworks.
        yaml_files: List of yaml file names.
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
    print("Will run the following yaml files:")
    for yaml_file in yaml_files:
        print("->", yaml_file)

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
        return experiment["config"].get("evaluation_interval", None) is not None

    # Loop through all collected files and gather experiments.
    # Set correct framework(s).
    for yaml_file in yaml_files:
        tf_experiments = yaml.safe_load(open(yaml_file).read())

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
                "episode_reward_mean"
                if not check_eval
                else "evaluation/episode_reward_mean"
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
                    "training_iteration": "iter",
                    "time_total_s": "time_total_s",
                    NUM_ENV_STEPS_SAMPLED: "ts (sampled)",
                    NUM_ENV_STEPS_TRAINED: "ts (trained)",
                    "episodes_this_iter": "train_episodes",
                    "episode_reward_mean": "reward_mean",
                    "evaluation/episode_reward_mean": "eval_reward_mean",
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
                    episode_reward_mean = np.mean(
                        [
                            t.metric_analysis["evaluation/episode_reward_mean"]["max"]
                            for t in trials_for_experiment
                        ]
                    )
                else:
                    episode_reward_mean = np.mean(
                        [
                            t.metric_analysis["episode_reward_mean"]["max"]
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
                    "episode_reward_mean": float(episode_reward_mean),
                    "throughput": (
                        float(throughput) if throughput is not None else 0.0
                    ),
                }

                print(
                    f" ... Desired reward={desired_reward}; "
                    f"desired throughput={desired_throughput}"
                )

                # We failed to reach desired reward or the desired throughput.
                if (desired_reward and episode_reward_mean < desired_reward) or (
                    desired_throughput and throughput < desired_throughput
                ):
                    print(
                        " ... Not successful: Actual "
                        f"reward={episode_reward_mean}; "
                        f"actual throughput={throughput}"
                    )
                    checks[experiment]["failures"] += 1
                # We succeeded!
                else:
                    print(
                        " ... Successful: (mark ok). Actual "
                        f"reward={episode_reward_mean}; "
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

    assert type(batch1) == type(
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

    if type(batch1) == SampleBatch:
        check_sample_batches(batch1, batch2)
    elif type(batch1) == MultiAgentBatch:
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
        num_envs_per_worker: 2

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

    stop_dict = {
        "training_iteration": training_iteration,
    }
    # use 0 and 2 workers (for more that 4 workers we have to make sure the instance
    # type in ci build has enough resources)
    for num_workers in [0, 2]:
        algo_config = (
            algo_config.debugging(seed=42)
            .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
            .rollouts(num_rollout_workers=num_workers, num_envs_per_worker=2)
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
            check(results1["hist_stats"], results2["hist_stats"])
            # As well as training behavior (minibatch sequence during SGD
            # iterations).
            check(
                results1["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
                results2["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
            )
