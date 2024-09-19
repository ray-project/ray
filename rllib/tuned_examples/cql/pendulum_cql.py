from pathlib import Path

from ray.rllib.algorithms.cql.cql import CQLConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args()
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

assert (
    args.env == "Pendulum-v1" or args.env is None
), "This tuned example works only with `Pendulum-v1`."

# Define the base path relative to this file.
base_path = Path(__file__).parents[2]
# Use the larger data set of Pendulum we have. Note, these are
# parquet data, the default in `AlgorithmConfig.offline_data`.
data_path = base_path / "tests/data/pendulum/pendulum-v1_enormous"

# Define the configuration.
config = (
    CQLConfig()
    .environment("Pendulum-v1")
    # Use the new API stack.
    .api_stack(
        enable_env_runner_and_connector_v2=True,
        enable_rl_module_and_learner=True,
    )
    .offline_data(
        input_=[data_path.as_posix()],
        # The `kwargs` for the `input_read_method`. We override the
        # the number of blocks to pull at once b/c our dataset is
        # small.
        input_read_method_kwargs={"override_num_blocks": max(args.num_gpus * 2, 2)},
        # The `kwargs` for the `map_batches` method in which our
        # `OfflinePreLearner` is run. 2 data workers should be run
        # concurrently.
        map_batches_kwargs={"concurrency": 2, "num_cpus": 2},
        # The `kwargs` for the `iter_batches` method. Due to the small
        # dataset we choose only a single batch to prefetch.
        iter_batches_kwargs={"prefetch_batches": 1},
        # The number of iterations to be run per learner when in multi-learner
        # mode in a single RLlib training iteration. Leave this to `None` to
        # run an entire epoch on the dataset during a single RLlib training
        # iteration. For single-learner mode 1 is the only option.
        dataset_num_iters_per_learner=1 if args.num_gpus == 0 else None,
        # TODO (sven): Has this any influence in the connectors?
        actions_in_input_normalized=True,
    )
    .training(
        bc_iters=200,
        tau=9.5e-3,
        min_q_weight=5.0,
        train_batch_size_per_learner=1024,
        twin_q=True,
        actor_lr=1.7e-3 * (args.num_gpus or 1) ** 0.5,
        critic_lr=2.5e-3 * (args.num_gpus or 1) ** 0.5,
        alpha_lr=1e-3 * (args.num_gpus or 1) ** 0.5,
        # Set this to `None` for all `SAC`-like algorithms. These
        # algorithms use learning rates for each optimizer.
        lr=None,
    )
    .reporting(
        min_time_s_per_iteration=10,
        metrics_num_episodes_for_smoothing=5,
    )
    .evaluation(
        evaluation_interval=3,
        evaluation_num_env_runners=1,
        evaluation_duration=5,
        evaluation_config={
            "explore": False,
        },
    )
)

stop = {
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": -700.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 800000,
}

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args, stop=stop)
