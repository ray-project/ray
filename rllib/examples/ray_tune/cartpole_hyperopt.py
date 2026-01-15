"""Hyperparameter tuning script for APPO on CartPole using HyperOpt.

This script uses Ray Tune's HyperOpt search algorithm to optimize APPO
hyperparameters for CartPole-v1, targeting convergence within 2 million
timesteps. HyperOpt uses Tree-structured Parzen Estimators (TPE) for
efficient Bayesian optimization of the hyperparameter search space.

The script runs 4 parallel trials by default, with HyperOpt suggesting new
hyperparameter configurations based on results from completed trials.
For each trial, it defaults to using a 1 GPU per learner, meaning that
you need to be running on a cluster with 4 GPUs available.
Due to this compute requirement, we recommend users run this script within an
anyscale job on an AWS with a g6.12xlarge worker node. Otherwise, we recommend
users change the num_gpus_per_learner to zero or the max_concurrent_trials to one.

Key hyperparameters being tuned:
- lr: Learning rate
- entropy_coeff: Entropy coefficient for exploration
- vf_loss_coeff: Value function loss coefficient
- train_batch_size_per_learner: Batch size per learner
- circular_buffer_num_batches: Number of batches in circular buffer
- circular_buffer_iterations_per_batch: Replay iterations per batch
- target_network_update_freq: Target network update frequency
- broadcast_interval: Weight synchronization interval

Note on storage for multi-node clusters
---------------------------------------
Ray Tune requires centralized storage accessible by all nodes in a multi-node cluster.
This script defaults to using the ANYSCALE_ARTIFACT_STORAGE environment variable,
which is automatically set in Anyscale jobs. For other environments, override with:
`python cartpole_hyperopt.py --storage-path=s3://my-bucket/path`
For local development, override with a local path:
`python cartpole_hyperopt.py --storage-path=~/ray_results`
See https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html for more details.

How to run this script
----------------------
Run with 4 parallel trials (default):
`python cartpole_hyperopt.py`

Run with custom number of parallel trials (max-concurrent-trials) and
the total number of trials (num_samples):
`python cartpole_hyperopt.py --max-concurrent-trials=2 --num_samples=20`

Run on a cluster with cloud or shared filesystem storage:
`python cartpole_hyperopt.py --storage-path=s3://my-bucket/appo-hyperopt`
`python cartpole_hyperopt.py --storage-path=/mnt/nfs/appo-hyperopt`

Results to expect
-----------------
HyperOpt will explore the hyperparameter space and converge toward configurations
that achieve reward of 475+ on CartPole within 2 million timesteps. The best
trial's hyperparameters will be logged at the end of training.
"""
import os

from ray import tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
)
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.tune import CLIReporter
from ray.tune.search import ConcurrencyLimiter
from ray.tune.search.hyperopt import HyperOptSearch

parser = add_rllib_example_script_args(
    default_reward=475.0,
    default_timesteps=2_000_000,
)
parser.add_argument(
    "--storage-path",
    default=os.environ.get("ANYSCALE_ARTIFACT_STORAGE"),
    help="The storage path for checkpoints and related tuning data.",
)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=6,
    num_learners=1,
    num_gpus_per_learner=1,
    num_samples=40,  # Run 40 training sweeps
    max_concurrent_trials=4,  # Run all 4 in parallel
)
args = parser.parse_args()


config = (
    APPOConfig()
    .environment("CartPole-v1")
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
    )
    .learners(
        num_learners=args.num_learners,
        num_gpus_per_learner=args.num_gpus_per_learner,
        num_aggregator_actors_per_learner=2,
    )
    .training(
        # Hyperparameters to tune with initial random values
        # Use tune.uniform for continuous params
        lr=tune.loguniform(0.0001, 0.005),
        vf_loss_coeff=tune.uniform(0.5, 2.0),
        entropy_coeff=tune.uniform(0.001, 0.02),
        # Use tune.qrandint(a, b, q) for discrete params in [a, b) with step q (defaults to 1)
        train_batch_size_per_learner=tune.qrandint(256, 2024, 64),
        target_network_update_freq=tune.qrandint(1, 6),
        broadcast_interval=tune.qrandint(2, 11),
        circular_buffer_num_batches=tune.qrandint(2, 6),
        circular_buffer_iterations_per_batch=tune.qrandint(1, 5),
    )
)

# Stopping criteria: either reach target reward or max timesteps
stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    TRAINING_ITERATION: args.stop_iters,
}


if __name__ == "__main__":
    # HyperOptSearch for Bayesian hyperparameter optimization using TPE.
    # Uses Tree-structured Parzen Estimators to intelligently explore the
    # hyperparameter space based on results from previous trials.
    # ConcurrencyLimiter ensures we don't exceed max_concurrent_trials.
    hyperopt_search = HyperOptSearch(
        metric=f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
        mode="max",
    )

    # Wrap search algorithm with ConcurrencyLimiter to control parallelism.
    search_alg = ConcurrencyLimiter(
        hyperopt_search, max_concurrent=args.max_concurrent_trials
    )

    tuner = tune.Tuner(
        config.algo_class,
        param_space=config,
        run_config=tune.RunConfig(
            stop=stop,
            storage_path=args.storage_path,
            checkpoint_config=tune.CheckpointConfig(
                checkpoint_at_end=True,
            ),
            progress_reporter=CLIReporter(
                metric_columns={
                    TRAINING_ITERATION: "iter",
                    "time_total_s": "total time (s)",
                    NUM_ENV_STEPS_SAMPLED_LIFETIME: "ts",
                    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": "episode return mean",
                },
                max_report_frequency=30,
            ),
        ),
        tune_config=tune.TuneConfig(
            metric=f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
            mode="max",
            num_samples=args.num_samples,
            search_alg=search_alg,
        ),
    )
    results = tuner.fit()
    print("Best hyperparameters:", results.get_best_result().config)
