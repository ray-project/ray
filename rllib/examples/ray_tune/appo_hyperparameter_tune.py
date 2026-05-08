"""Hyperparameter tuning script for APPO on CartPole using BasicVariantGenerator.

This script uses Ray Tune's BasicVariantGenerator to perform grid/random search
over APPO hyperparameters for CartPole-v1 (though is applicable to any RLlib algorithm).

BasicVariantGenerator is Tune's default search algorithm that generates trial
configurations from the search space without using historical trial results.
It supports grid search (tune.grid_search), random sampling (tune.uniform, etc.),
and combinations thereof.

Alternative Search Algorithms
-----------------------------
Ray Tune supports many search algorithms that can leverage results from previous
trials to guide the search more efficiently:

- HyperOptSearch: Bayesian optimization using Tree-structured Parzen Estimators (TPE)
- OptunaSearch: Bayesian optimization with pruning support via Optuna
- BayesOptSearch: Gaussian process-based Bayesian optimization
- AxSearch: Adaptive experimentation platform from Meta
- BlendSearch/CFO: Cost-aware optimization algorithms from Microsoft FLAML
- BOHB: Bayesian Optimization and HyperBand
- Nevergrad: Derivative-free optimization
- ZOOpt: Zeroth-order optimization

See the full list and usage examples at:
https://docs.ray.io/en/latest/tune/api/suggestion.html

Note: When using these advanced search algorithms, wrap them with ConcurrencyLimiter
to control parallelism (e.g., `ConcurrencyLimiter(HyperOptSearch(), max_concurrent=4)`).
BasicVariantGenerator has built-in concurrency control via its `max_concurrent` parameter.

The script runs 4 parallel trials by default.
For each trial, it defaults to using 1 GPU per learner, meaning that
you need to be running on a cluster with 4 GPUs available.
Otherwise, we recommend users change `num_gpus_per_learner` to zero
or `max_concurrent_trials` to one (if only single GPU is available).

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
This can be an S3 bucket or local storage accessible to all nodes.
If running on an Anyscale job, it has an internal S3 bucket defined by the
ANYSCALE_ARTIFACT_STORAGE environment variable.
See https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html for more details.

How to run this script
----------------------
Run with 4 parallel trials (default):
`python appo_hyperparameter_tune.py`

Run with custom number of parallel trials (max-concurrent-trials) and
the total number of trials (num_samples):
`python appo_hyperparameter_tune.py --max-concurrent-trials=2 --num_samples=20`

Run on a cluster with cloud or local filesystem storage:
`python appo_hyperparameter_tune.py --storage-path=s3://my-bucket/appo-hyperopt`
`python appo_hyperparameter_tune.py --storage-path=/mnt/nfs/appo-hyperopt`

Run locally with only a single GPU
`python appo_hyperparameter_tune.py --max-concurrent-trials=1 --num_samples=5 --storage-path=/mnt/nfs/appo-hyperopt`

Results to expect
-----------------
The tuner will explore the hyperparameter space via random sampling and find
configurations that achieve reward of 475+ on CartPole within 2 million timesteps.
The best trial's hyperparameters will be logged at the end of training.
"""

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
from ray.tune.search import BasicVariantGenerator

parser = add_rllib_example_script_args(
    default_reward=475.0,
    default_timesteps=2_000_000,
)
parser.add_argument(
    "--storage-path",
    default="~/ray_results",
    type=str,
    help="The storage path for checkpoints and related tuning data.",
)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=6,
    num_learners=1,
    num_gpus_per_learner=1,
    num_samples=12,  # Run 12 training trials
    max_concurrent_trials=4,  # Run 4 trials in parallel
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
        train_batch_size_per_learner=tune.qrandint(256, 2048, 64),
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
}


if __name__ == "__main__":
    # BasicVariantGenerator generates trial configurations from the search space
    # without using historical trial results. It's Tune's default search algorithm
    # and supports grid search, random sampling, and combinations.
    # max_concurrent limits how many trials run in parallel.
    search_alg = BasicVariantGenerator(max_concurrent=args.max_concurrent_trials)

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
