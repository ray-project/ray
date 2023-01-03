"""Example of customizing evaluation with RLlib.

Pass --custom-eval to run with a custom evaluation function too.

Here we define a custom evaluation method that runs a specific sweep of env
parameters (SimpleCorridor corridor lengths).

------------------------------------------------------------------------
Sample output for `python custom_eval.py`
------------------------------------------------------------------------

INFO algorithm.py:623 -- Evaluating current policy for 10 episodes.
INFO algorithm.py:650 -- Running round 0 of parallel evaluation (2/10 episodes)
INFO algorithm.py:650 -- Running round 1 of parallel evaluation (4/10 episodes)
INFO algorithm.py:650 -- Running round 2 of parallel evaluation (6/10 episodes)
INFO algorithm.py:650 -- Running round 3 of parallel evaluation (8/10 episodes)
INFO algorithm.py:650 -- Running round 4 of parallel evaluation (10/10 episodes)

Result for PG_SimpleCorridor_2c6b27dc:
  ...
  evaluation:
    custom_metrics: {}
    episode_len_mean: 15.864661654135338
    episode_reward_max: 1.0
    episode_reward_mean: 0.49624060150375937
    episode_reward_min: 0.0
    episodes_this_iter: 133
    off_policy_estimator: {}
    policy_reward_max: {}
    policy_reward_mean: {}
    policy_reward_min: {}
    sampler_perf:
      mean_env_wait_ms: 0.0362923321333299
      mean_inference_ms: 0.6319202064080927
      mean_processing_ms: 0.14143652169068222

------------------------------------------------------------------------
Sample output for `python custom_eval.py --custom-eval`
------------------------------------------------------------------------

INFO algorithm.py:631 -- Running custom eval function <function ...>
Update corridor length to 4
Update corridor length to 7
Custom evaluation round 1
Custom evaluation round 2
Custom evaluation round 3
Custom evaluation round 4

Result for PG_SimpleCorridor_0de4e686:
  ...
  evaluation:
    custom_metrics: {}
    episode_len_mean: 9.15695067264574
    episode_reward_max: 1.0
    episode_reward_mean: 0.9596412556053812
    episode_reward_min: 0.0
    episodes_this_iter: 223
    foo: 1
    off_policy_estimator: {}
    policy_reward_max: {}
    policy_reward_mean: {}
    policy_reward_min: {}
    sampler_perf:
      mean_env_wait_ms: 0.03423667269562796
      mean_inference_ms: 0.5654563161491506
      mean_processing_ms: 0.14494765630060774
"""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.pg import PGConfig
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.examples.env.simple_corridor import SimpleCorridor
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--evaluation-parallel-to-training", action="store_true")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--no-custom-eval", action="store_true")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=50, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=20000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=0.7, help="Reward at which we stop training."
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.",
)


def custom_eval_function(algorithm, eval_workers):
    """Example of a custom evaluation function.

    Args:
        algorithm: Algorithm class to evaluate.
        eval_workers: Evaluation WorkerSet.

    Returns:
        metrics: Evaluation metrics dict.
    """

    # We configured 2 eval workers in the training config.
    funcs = [
        lambda w: w.foreach_env(lambda env: env.set_corridor_length(4)),
        lambda w: w.foreach_env(lambda env: env.set_corridor_length(7)),
    ]

    # Set different env settings for each worker. Here we use a fixed config,
    # which also could have been computed in each worker by looking at
    # env_config.worker_index (printed in SimpleCorridor class above).
    eval_workers.foreach_worker(func=funcs)

    for i in range(5):
        print("Custom evaluation round", i)
        # Calling .sample() runs exactly one episode per worker due to how the
        # eval workers are configured.
        eval_workers.foreach_worker(func=lambda w: w.sample())

    # Collect the accumulated episodes on the workers, and then summarize the
    # episode stats into a metrics dict.
    episodes = collect_episodes(workers=eval_workers, timeout_seconds=99999)
    # You can compute metrics from the episodes manually, or use the
    # convenient `summarize_episodes()` utility:
    metrics = summarize_episodes(episodes)

    # You can also put custom values in the metrics dict.
    metrics["foo"] = 1
    return metrics


if __name__ == "__main__":
    args = parser.parse_args()

    if args.no_custom_eval:
        eval_fn = None
    else:
        eval_fn = custom_eval_function

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    config = (
        PGConfig()
        .environment(SimpleCorridor, env_config={"corridor_length": 10})
        # Training rollouts will be collected using just the learner
        # process, but evaluation will be done in parallel with two
        # workers. Hence, this run will use 3 CPUs total (1 for the
        # learner + 2 more for evaluation workers).
        .rollouts(num_rollout_workers=0)
        .evaluation(
            evaluation_num_workers=2,
            # Enable evaluation, once per training iteration.
            evaluation_interval=1,
            # Run 10 episodes each time evaluation runs (OR "auto" if parallel to
            # training).
            evaluation_duration="auto" if args.evaluation_parallel_to_training else 10,
            # Evaluate parallelly to training.
            evaluation_parallel_to_training=args.evaluation_parallel_to_training,
            evaluation_config=PGConfig.overrides(
                env_config={
                    # Evaluate using LONGER corridor than trained on.
                    "corridor_length": 5,
                },
            ),
            custom_evaluation_function=eval_fn,
        )
        .framework(args.framework)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=float(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        "PG",
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=1),
    )
    results = tuner.fit()

    # Check eval results (from eval workers using the custom function),
    # not results from the regular workers.
    if args.as_test:
        check_learning_achieved(results, args.stop_reward, evaluation=True)
    ray.shutdown()
