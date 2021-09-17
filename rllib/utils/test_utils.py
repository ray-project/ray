from collections import Counter
import copy
import gym
import logging
import numpy as np
import re
import time
from typing import Any, Dict, List
import yaml

import ray
from ray.rllib.utils.framework import try_import_jax, try_import_tf, \
    try_import_torch
from ray.tune import run_experiments

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


def framework_iterator(config=None,
                       frameworks=("tf2", "tf", "tfe", "torch"),
                       session=False):
    """An generator that allows for looping through n frameworks for testing.

    Provides the correct config entries ("framework") as well
    as the correct eager/non-eager contexts for tfe/tf.

    Args:
        config (Optional[dict]): An optional config dict to alter in place
            depending on the iteration.
        frameworks (Tuple[str]): A list/tuple of the frameworks to be tested.
            Allowed are: "tf2", "tf", "tfe", "torch", and None.
        session (bool): If True and only in the tf-case: Enter a tf.Session()
            and yield that as second return value (otherwise yield (fw, None)).
            Also sets a seed (42) on the session to make the test
            deterministic.

    Yields:
        str: If enter_session is False:
            The current framework ("tf2", "tf", "tfe", "torch") used.
        Tuple(str, Union[None,tf.Session]: If enter_session is True:
            A tuple of the current fw and the tf.Session if fw="tf".
    """
    config = config or {}
    frameworks = [frameworks] if isinstance(frameworks, str) else \
        list(frameworks)

    # Both tf2 and tfe present -> remove "tfe" or "tf2" depending on version.
    if "tf2" in frameworks and "tfe" in frameworks:
        frameworks.remove("tfe" if tfv == 2 else "tf2")

    for fw in frameworks:
        # Skip non-installed frameworks.
        if fw == "torch" and not torch:
            logger.warning(
                "framework_iterator skipping torch (not installed)!")
            continue
        if fw != "torch" and not tf:
            logger.warning("framework_iterator skipping {} (tf not "
                           "installed)!".format(fw))
            continue
        elif fw == "tfe" and not eager_mode:
            logger.warning("framework_iterator skipping tf-eager (could not "
                           "import `eager_mode` from tensorflow.python)!")
            continue
        elif fw == "tf2" and tfv != 2:
            logger.warning(
                "framework_iterator skipping tf2.x (tf version is < 2.0)!")
            continue
        elif fw == "jax" and not jax:
            logger.warning("framework_iterator skipping JAX (not installed)!")
            continue
        assert fw in ["tf2", "tf", "tfe", "torch", "jax", None]

        # Do we need a test session?
        sess = None
        if fw == "tf" and session is True:
            sess = tf1.Session()
            sess.__enter__()
            tf1.set_random_seed(42)

        print("framework={}".format(fw))

        config["framework"] = fw

        eager_ctx = None
        # Enable eager mode for tf2 and tfe.
        if fw in ["tf2", "tfe"]:
            eager_ctx = eager_mode()
            eager_ctx.__enter__()
            assert tf1.executing_eagerly()
        # Make sure, eager mode is off.
        elif fw == "tf":
            assert not tf1.executing_eagerly()

        yield fw if session is False else (fw, sess)

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
        x (any): The value to be compared (to the expectation: `y`). This
            may be a Tensor.
        y (any): The expected value to be compared to `x`. This must not
            be a tf-Tensor, but may be a tfe/torch-Tensor.
        decimals (int): The number of digits after the floating point up to
            which all numeric values have to match.
        atol (float): Absolute tolerance of the difference between x and y
            (overrides `decimals` if given).
        rtol (float): Relative tolerance of the difference between x and y
            (overrides `decimals` if given).
        false (bool): Whether to check that x and y are NOT the same.
    """
    # A dict type.
    if isinstance(x, dict):
        assert isinstance(y, dict), \
            "ERROR: If x is dict, y needs to be a dict as well!"
        y_keys = set(x.keys())
        for key, value in x.items():
            assert key in y, \
                "ERROR: y does not have x's key='{}'! y={}".format(key, y)
            check(
                value,
                y[key],
                decimals=decimals,
                atol=atol,
                rtol=rtol,
                false=false)
            y_keys.remove(key)
        assert not y_keys, \
            "ERROR: y contains keys ({}) that are not in x! y={}".\
            format(list(y_keys), y)
    # A tuple type.
    elif isinstance(x, (tuple, list)):
        assert isinstance(y, (tuple, list)),\
            "ERROR: If x is tuple, y needs to be a tuple as well!"
        assert len(y) == len(x),\
            "ERROR: y does not have the same length as x ({} vs {})!".\
            format(len(y), len(x))
        for i, value in enumerate(x):
            check(
                value,
                y[i],
                decimals=decimals,
                atol=atol,
                rtol=rtol,
                false=false)
    # Boolean comparison.
    elif isinstance(x, (np.bool_, bool)):
        if false is True:
            assert bool(x) is not bool(y), \
                "ERROR: x ({}) is y ({})!".format(x, y)
        else:
            assert bool(x) is bool(y), \
                "ERROR: x ({}) is not y ({})!".format(x, y)
    # Nones or primitives.
    elif x is None or y is None or isinstance(x, (str, int)):
        if false is True:
            assert x != y, "ERROR: x ({}) is the same as y ({})!".format(x, y)
        else:
            assert x == y, \
                "ERROR: x ({}) is not the same as y ({})!".format(x, y)
    # String/byte comparisons.
    elif hasattr(x, "dtype") and \
            (x.dtype == np.object or str(x.dtype).startswith("<U")):
        try:
            np.testing.assert_array_equal(x, y)
            if false is True:
                assert False, \
                    "ERROR: x ({}) is the same as y ({})!".format(x, y)
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
                        "Use numpy.ndarray instead")
            if isinstance(x, (tf1.Tensor, tf1.Variable)):
                # In eager mode, numpyize tensors.
                if tf1.executing_eagerly():
                    x = x.numpy()
                # Otherwise, use a new tf-session.
                else:
                    with tf1.Session() as sess:
                        x = sess.run(x)
                        return check(
                            x,
                            y,
                            decimals=decimals,
                            atol=atol,
                            rtol=rtol,
                            false=false)
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
                    assert False, \
                        "ERROR: x ({}) is the same as y ({})!".format(x, y)

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
                    assert False, \
                        "ERROR: x ({}) is the same as y ({})!".format(x, y)


def check_learning_achieved(tune_results, min_reward, evaluation=False):
    """Throws an error if `min_reward` is not reached within tune_results.

    Checks the last iteration found in tune_results for its
    "episode_reward_mean" value and compares it to `min_reward`.

    Args:
        tune_results: The tune.run returned results object.
        min_reward (float): The min reward that must be reached.

    Raises:
        ValueError: If `min_reward` not reached.
    """
    # Get maximum reward of all trials
    # (check if at least one trial achieved some learning)
    avg_rewards = [(trial.last_result["episode_reward_mean"]
                    if not evaluation else
                    trial.last_result["evaluation"]["episode_reward_mean"])
                   for trial in tune_results.trials]
    best_avg_reward = max(avg_rewards)
    if best_avg_reward < min_reward:
        raise ValueError("`stop-reward` of {} not reached!".format(min_reward))
    print("ok")


def check_compute_single_action(trainer,
                                include_state=False,
                                include_prev_action_reward=False):
    """Tests different combinations of args for trainer.compute_single_action.

    Args:
        trainer (Trainer): The Trainer object to test.
        include_state (bool): Whether to include the initial state of the
            Policy's Model in the `compute_single_action` call.
        include_prev_action_reward (bool): Whether to include the prev-action
            and -reward in the `compute_single_action` call.

    Raises:
        ValueError: If anything unexpected happens.
    """
    try:
        pol = trainer.get_policy()
    except AttributeError:
        pol = trainer.policy
    model = pol.model

    action_space = pol.action_space

    for what in [pol, trainer]:
        if what is trainer:
            method_to_test = trainer.compute_single_action
            # Get the obs-space from Workers.env (not Policy) due to possible
            # pre-processor up front.
            worker_set = getattr(trainer, "workers",
                                 getattr(trainer, "_workers", None))
            assert worker_set
            if isinstance(worker_set, list):
                obs_space = trainer.get_policy().observation_space
            else:
                obs_space = worker_set.local_worker().for_policy(
                    lambda p: p.observation_space)
            obs_space = getattr(obs_space, "original_space", obs_space)
        else:
            method_to_test = pol.compute_single_action
            obs_space = pol.observation_space

        for explore in [True, False]:
            for full_fetch in ([False, True] if what is trainer else [False]):
                call_kwargs = {}
                if what is trainer:
                    call_kwargs["full_fetch"] = full_fetch
                else:
                    call_kwargs["clip_actions"] = True

                obs = obs_space.sample()
                if isinstance(obs_space, gym.spaces.Box):
                    obs = np.clip(obs, -1.0, 1.0)
                state_in = None
                if include_state:
                    state_in = model.get_initial_state()
                    if not state_in:
                        state_in = []
                        i = 0
                        while f"state_in_{i}" in model.view_requirements:
                            state_in.append(model.view_requirements[
                                f"state_in_{i}"].space.sample())
                            i += 1
                action_in = action_space.sample() \
                    if include_prev_action_reward else None
                reward_in = 1.0 if include_prev_action_reward else None
                action = method_to_test(
                    obs,
                    state_in,
                    prev_action=action_in,
                    prev_reward=reward_in,
                    explore=explore,
                    **call_kwargs)

                state_out = None
                if state_in or full_fetch or what is pol:
                    action, state_out, _ = action
                if state_out:
                    for si, so in zip(state_in, state_out):
                        check(list(si.shape), so.shape)

                if not action_space.contains(action):
                    raise ValueError(
                        "Returned action ({}) of trainer/policy {} not in "
                        "Env's action_space "
                        "({})!".format(action, what, action_space))


def run_learning_tests_from_yaml(
        yaml_files: List[str],
        *,
        max_num_repeats: int = 2,
        smoke_test: bool = False,
) -> Dict[str, Any]:
    """Runs the given experiments in yaml_files and returns results dict.

    Args:
        yaml_files (List[str]): List of yaml file names.
        max_num_repeats (int): How many times should we repeat a failed
            experiment?
        smoke_test (bool): Whether this is just a smoke-test. If True,
            set time_total_s to 5min and don't early out due to rewards
            or timesteps reached.
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

    start_time = time.monotonic()

    # Loop through all collected files and gather experiments.
    # Augment all by `torch` framework.
    for yaml_file in yaml_files:
        tf_experiments = yaml.load(open(yaml_file).read())

        # Add torch version of all experiments to the list.
        for k, e in tf_experiments.items():
            # If framework explicitly given, only test for that framework.
            # Some algos do not have both versions available.
            if "framework" in e["config"]:
                frameworks = [e["config"]["framework"]]
            else:
                frameworks = ["tf", "torch"]
                e["config"]["framework"] = "tf"

            # For smoke-tests, we just run for n min.
            if smoke_test:
                # 0sec for each(!) experiment/trial.
                # This is such that if there are many experiments/trials
                # in a test (e.g. rllib_learning_test), each one can at least
                # create its trainer and run a first iteration.
                e["stop"]["time_total_s"] = 0
            else:
                # We also stop early, once we reach the desired reward.
                e["stop"]["episode_reward_mean"] = \
                    e["pass_criteria"]["episode_reward_mean"]

            keys = []
            # Generate the torch copy of the experiment.
            if len(frameworks) == 2:
                e_torch = copy.deepcopy(e)
                e_torch["config"]["framework"] = "torch"
                keys.append(re.sub("^(\\w+)-", "\\1-tf-", k))
                keys.append(re.sub("-tf-", "-torch-", keys[0]))
                experiments[keys[0]] = e
                experiments[keys[1]] = e_torch
            # tf-only.
            elif frameworks[0] == "tf":
                keys.append(re.sub("^(\\w+)-", "\\1-tf-", k))
                experiments[keys[0]] = e
            # torch-only.
            else:
                keys.append(re.sub("^(\\w+)-", "\\1-torch-", k))
                experiments[keys[0]] = e

            # Generate `checks` dict for all experiments (tf and/or torch).
            for k_ in keys:
                e = experiments[k_]
                checks[k_] = {
                    "min_reward": e["pass_criteria"]["episode_reward_mean"],
                    "min_timesteps": e["pass_criteria"]["timesteps_total"],
                    "time_total_s": e["stop"]["time_total_s"],
                    "failures": 0,
                    "passed": False,
                }
                # This key would break tune.
                e.pop("pass_criteria", None)

    # Print out the actual config.
    print("== Test config ==")
    print(yaml.dump(experiments))

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

        # Run remaining experiments.
        trials = run_experiments(experiments_to_run, resume=False, verbose=2)
        all_trials.extend(trials)

        # Check each trial for whether we passed.
        # Criteria is to a) reach reward AND b) to have reached the throughput
        # defined by `timesteps_total` / `time_total_s`.
        for t in trials:
            experiment = re.sub(".+/([^/]+)$", "\\1", t.local_dir)

            # If we have evaluation workers, use their rewards.
            # This is useful for offline learning tests, where
            # we evaluate against an actual environment.
            check_eval = experiments[experiment]["config"].get(
                "evaluation_interval", None) is not None

            # Error: Increase failure count and repeat.
            if t.status == "ERROR":
                checks[experiment]["failures"] += 1
            # Smoke-tests always succeed.
            elif smoke_test:
                checks[experiment]["passed"] = True
                del experiments_to_run[experiment]
            # Experiment finished: Check reward achieved and timesteps done
            # (throughput).
            else:
                reward_mean = \
                    t.last_result["evaluation"]["episode_reward_mean"] if \
                    check_eval else t.last_result["episode_reward_mean"]
                desired_reward = checks[experiment]["min_reward"]

                throughput = t.last_result["timesteps_total"] / \
                    t.last_result["time_total_s"]
                desired_timesteps = checks[experiment]["min_timesteps"]
                desired_throughput = \
                    desired_timesteps / t.stopping_criterion["time_total_s"]

                # We failed to reach desired reward or the desired throughput.
                if reward_mean < desired_reward or \
                    (desired_throughput and
                     throughput < desired_throughput):
                    checks[experiment]["failures"] += 1
                # We succeeded!
                else:
                    checks[experiment]["passed"] = True
                    del experiments_to_run[experiment]

    ray.shutdown()

    time_taken = time.monotonic() - start_time

    # Create results dict and write it to disk.
    result = {
        "time_taken": time_taken,
        "trial_states": dict(Counter([trial.status for trial in all_trials])),
        "last_update": time.time(),
        "passed": [k for k, exp in checks.items() if exp["passed"]],
        "failures": {
            k: exp["failures"]
            for k, exp in checks.items() if exp["failures"] > 0
        }
    }

    return result
