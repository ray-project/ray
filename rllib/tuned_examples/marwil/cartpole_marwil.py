from pathlib import Path

from ray.rllib.algorithms.marwil import MARWILConfig
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
    args.env == "CartPole-v1" or args.env is None
), "This tuned example works only with `CartPole-v1`."

# Define the data paths.
data_path = "tests/data/cartpole/cartpole-v1_large"
base_path = Path(__file__).parents[2]
print(f"base_path={base_path}")
data_path = "local://" / base_path / data_path
print(f"data_path={data_path}")

# Define the MARWIL config.
config = (
    MARWILConfig()
    .environment(env="CartPole-v1")
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .evaluation(
        evaluation_interval=3,
        evaluation_num_env_runners=1,
        evaluation_duration=5,
        evaluation_parallel_to_training=True,
    )
    # Note, the `input_` argument is the major argument for the
    # new offline API. Via the `input_read_method_kwargs` the
    # arguments for the `ray.data.Dataset` read method can be
    # configured. The read method needs at least as many blocks
    # as remote learners.
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
    )
    .training(
        beta=1.0,
        # To increase learning speed with multiple learners,
        # increase the learning rate correspondingly.
        lr=0.0008 * max(1, args.num_gpus**0.5),
        train_batch_size_per_learner=1024,
    )
)

stop = {
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 250.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 500000,
}

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args, stop=stop)
