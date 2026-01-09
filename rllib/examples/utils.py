import argparse
import json
import logging
import os
import re
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Type,
    Union,
)

import numpy as np

import ray
from ray import tune
from ray.air.integrations.wandb import WANDB_ENV_VAR, WandbLoggerCallback
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.serialization import convert_numpy_to_python_primitives
from ray.rllib.utils.typing import ResultDict
from ray.tune import CLIReporter
from ray.tune.result import TRAINING_ITERATION

if TYPE_CHECKING:
    from ray.rllib.algorithms import AlgorithmConfig

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
        "--num-envs-per-env-runner",
        type=int,
        default=None,
        help="The number of (vectorized) environments per EnvRunner. Note that "
        "this is identical to the batch size for (inference) action computations.",
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
        "--max-concurrent-trials",
        type=int,
        default=None,
        help="How many (tune.Tuner) trials to run concurrently.",
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
    parser.add_argument(
        "--num-learners",
        type=int,
        default=None,
        help="The number of Learners to use. If `None`, use the algorithm's default "
        "value.",
    )
    parser.add_argument(
        "--num-cpus-per-learner",
        type=float,
        default=None,
        help="The number of CPUs per Learner to use. If `None`, use the algorithm's "
        "default value.",
    )
    parser.add_argument(
        "--num-gpus-per-learner",
        type=float,
        default=None,
        help="The number of GPUs per Learner to use. If `None` and there are enough "
        "GPUs for all required Learners (--num-learners), use a value of 1, "
        "otherwise 0.",
    )
    parser.add_argument(
        "--num-aggregator-actors-per-learner",
        type=int,
        default=None,
        help="The number of Aggregator actors to use per Learner. If `None`, use the "
        "algorithm's default value.",
    )

    # Ray init options.
    parser.add_argument("--num-cpus", type=int, default=0)
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.",
    )

    # Old API stack: config.num_gpus.
    parser.add_argument(
        "--num-gpus",
        type=int,
        default=None,
        help="The number of GPUs to use (only on the old API stack).",
    )
    parser.add_argument(
        "--old-api-stack",
        action="store_true",
        help="Run this script on the old API stack of RLlib.",
    )

    # Deprecated options. Throws error when still used. Use `--old-api-stack` for
    # disabling the new API stack.
    parser.add_argument(
        "--enable-new-api-stack",
        action="store_true",
    )

    return parser


# TODO (simon): Use this function in the `run_rllib_example_experiment` when
# `no_tune` is `True`.
def should_stop(
    stop: Dict[str, Any], results: ResultDict, keep_ray_up: bool = False
) -> bool:
    """Checks stopping criteria on `ResultDict`

    Args:
        stop: Dictionary of stopping criteria. Each criterium is a mapping of
            a metric in the `ResultDict` of the algorithm to a certain criterium.
        results: An RLlib `ResultDict` containing all results from a training step.
        keep_ray_up: Optionally shutting down the runnin Ray instance.

    Returns: True, if any stopping criterium is fulfilled. Otherwise, False.
    """
    for key, threshold in stop.items():
        val = results
        for k in key.split("/"):
            k = k.strip()
            # If k exists in the current level, continue down;
            # otherwise, set val to None and break out of this inner loop.
            if isinstance(val, dict) and k in val:
                val = val[k]
            else:
                val = None
                break

        # If the key was not found, simply skip to the next criterion.
        if val is None:
            continue

        try:
            # Check that val is numeric and meets the threshold.
            if not np.isnan(val) and val >= threshold:
                print(f"Stop criterion ({key}={threshold}) fulfilled!")
                if not keep_ray_up:
                    ray.shutdown()
                return True
        except TypeError:
            # If val isn't numeric, skip this criterion.
            continue

    # If none of the criteria are fulfilled, return False.
    return False


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
    keep_ray_up: bool = False,
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
            that are otherwise taken care of automatically in this function (e.g.
            `num_env_runners`).

    Returns:
        The last ResultDict from a --no-tune run OR the tune.Tuner.fit()
        results.
    """
    if args is None:
        parser = add_rllib_example_script_args()
        args = parser.parse_args()

    # Deprecated args.
    if args.enable_new_api_stack:
        raise ValueError(
            "`--enable-new-api-stack` flag no longer supported (it's the default "
            "behavior now)! To switch back to the old API stack on your scripts, use "
            "the `--old-api-stack` flag."
        )

    # If run --as-release-test, --as-test must also be set.
    if args.as_release_test:
        args.as_test = True

    # Initialize Ray.
    ray.init(
        num_cpus=args.num_cpus or None,
        local_mode=args.local_mode,
        ignore_reinit_error=True,
    )

    # Define one or more stopping criteria.
    if stop is None:
        stop = {
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
            f"{ENV_RUNNER_RESULTS}/{NUM_ENV_STEPS_SAMPLED_LIFETIME}": (
                args.stop_timesteps
            ),
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

        # Disable the new API stack?
        if args.old_api_stack:
            config.api_stack(
                enable_rl_module_and_learner=False,
                enable_env_runner_and_connector_v2=False,
            )

        # Define EnvRunner scaling and behavior.
        if args.num_env_runners is not None:
            config.env_runners(num_env_runners=args.num_env_runners)
        if args.num_envs_per_env_runner is not None:
            config.env_runners(num_envs_per_env_runner=args.num_envs_per_env_runner)

        # Define compute resources used automatically (only using the --num-learners
        # and --num-gpus-per-learner args).
        # New stack.
        if config.enable_rl_module_and_learner:
            if args.num_gpus is not None and args.num_gpus > 0:
                raise ValueError(
                    "--num-gpus is not supported on the new API stack! To train on "
                    "GPUs, use the command line options `--num-gpus-per-learner=1` and "
                    "`--num-learners=[your number of available GPUs]`, instead."
                )

            # Do we have GPUs available in the cluster?
            num_gpus_available = ray.cluster_resources().get("GPU", 0)
            # Number of actual Learner instances (including the local Learner if
            # `num_learners=0`).
            num_actual_learners = (
                args.num_learners
                if args.num_learners is not None
                else config.num_learners
            ) or 1  # 1: There is always a local Learner, if num_learners=0.
            # How many were hard-requested by the user
            # (through explicit `--num-gpus-per-learner >= 1`).
            num_gpus_requested = (args.num_gpus_per_learner or 0) * num_actual_learners
            # Number of GPUs needed, if `num_gpus_per_learner=None` (auto).
            num_gpus_needed_if_available = (
                args.num_gpus_per_learner
                if args.num_gpus_per_learner is not None
                else 1
            ) * num_actual_learners
            # Define compute resources used.
            config.resources(num_gpus=0)  # old API stack setting
            if args.num_learners is not None:
                config.learners(num_learners=args.num_learners)

            # User wants to use aggregator actors per Learner.
            if args.num_aggregator_actors_per_learner is not None:
                config.learners(
                    num_aggregator_actors_per_learner=(
                        args.num_aggregator_actors_per_learner
                    )
                )

            # User wants to use GPUs if available, but doesn't hard-require them.
            if args.num_gpus_per_learner is None:
                if num_gpus_available >= num_gpus_needed_if_available:
                    config.learners(num_gpus_per_learner=1)
                else:
                    config.learners(num_gpus_per_learner=0)
            # User hard-requires n GPUs, but they are not available -> Error.
            elif num_gpus_available < num_gpus_requested:
                raise ValueError(
                    "You are running your script with --num-learners="
                    f"{args.num_learners} and --num-gpus-per-learner="
                    f"{args.num_gpus_per_learner}, but your cluster only has "
                    f"{num_gpus_available} GPUs!"
                )

            # All required GPUs are available -> Use them.
            else:
                config.learners(num_gpus_per_learner=args.num_gpus_per_learner)

            # Set CPUs per Learner.
            if args.num_cpus_per_learner is not None:
                config.learners(num_cpus_per_learner=args.num_cpus_per_learner)

        # Old stack (override only if arg was provided by user).
        elif args.num_gpus is not None:
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
                mean_return = results[ENV_RUNNER_RESULTS].get(
                    EPISODE_RETURN_MEAN, np.nan
                )
                print(f"iter={i} R={mean_return}", end="")
            if (
                EVALUATION_RESULTS in results
                and ENV_RUNNER_RESULTS in results[EVALUATION_RESULTS]
            ):
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
                    if not keep_ray_up:
                        ray.shutdown()
                    return results

        if not keep_ray_up:
            ray.shutdown()
        return results

    # Run the experiment using Ray Tune.

    # Log results using WandB.
    tune_callbacks = tune_callbacks or []
    if hasattr(args, "wandb_key") and (
        args.wandb_key is not None or WANDB_ENV_VAR in os.environ
    ):
        wandb_key = args.wandb_key or os.environ[WANDB_ENV_VAR]
        project = args.wandb_project or (
            args.algo.lower() + "-" + re.sub("\\W+", "-", str(config.env).lower())
        )
        tune_callbacks.append(
            WandbLoggerCallback(
                api_key=wandb_key,
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
        run_config=tune.RunConfig(
            stop=stop,
            verbose=args.verbose,
            callbacks=tune_callbacks,
            checkpoint_config=tune.CheckpointConfig(
                checkpoint_frequency=args.checkpoint_freq,
                checkpoint_at_end=args.checkpoint_at_end,
            ),
            progress_reporter=progress_reporter,
        ),
        tune_config=tune.TuneConfig(
            num_samples=args.num_samples,
            max_concurrent_trials=args.max_concurrent_trials,
            scheduler=scheduler,
        ),
    ).fit()
    time_taken = time.time() - start_time

    if not keep_ray_up:
        ray.shutdown()

    # Error out, if Tuner.fit() failed to run. Otherwise, erroneous examples might pass
    # the CI tests w/o us knowing that they are broken (b/c some examples do not have
    # a --as-test flag and/or any passing criteria).
    if results.errors:
        # Might cause an IndexError if the tuple is not long enough; in that case, use repr(e).
        errors = [
            e.args[0].args[2]
            if e.args and hasattr(e.args[0], "args") and len(e.args[0].args) > 2
            else repr(e)
            for e in results.errors
        ]
        raise RuntimeError(
            f"Running the example script resulted in one or more errors! {errors}"
        )

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
                "stats": convert_numpy_to_python_primitives(stats),
                "passed": [test_passed],
                "not_passed": [not test_passed],
                "failures": {str(trial): 1} if not test_passed else {},
            }
            filename = os.environ.get("TEST_OUTPUT_JSON", "/tmp/learning_test.json")
            with open(filename, "wt") as f:
                json.dump(json_summary, f)

        if not test_passed:
            raise ValueError(
                f"`{success_metric_key}` of {success_metric_value} not reached!"
            )

    return results
