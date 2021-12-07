"""Example of customizing evaluation with RLlib.

Pass --custom-eval to run with a custom evaluation function too.

Here we define a custom evaluation method that runs a specific sweep of env
parameters (SimpleCorridor corridor lengths).

------------------------------------------------------------------------
Sample output for `python custom_eval.py`
------------------------------------------------------------------------

INFO trainer.py:623 -- Evaluating current policy for 10 episodes.
INFO trainer.py:650 -- Running round 0 of parallel evaluation (2/10 episodes)
INFO trainer.py:650 -- Running round 1 of parallel evaluation (4/10 episodes)
INFO trainer.py:650 -- Running round 2 of parallel evaluation (6/10 episodes)
INFO trainer.py:650 -- Running round 3 of parallel evaluation (8/10 episodes)
INFO trainer.py:650 -- Running round 4 of parallel evaluation (10/10 episodes)

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

INFO trainer.py:631 -- Running custom eval function <function ...>
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
from ray import tune
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.examples.env.simple_corridor import SimpleCorridor
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument("--no-custom-eval", action="store_true")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.")
parser.add_argument(
    "--stop-iters",
    type=int,
    default=50,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=20000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=0.7,
    help="Reward at which we stop training.")


def custom_eval_function(trainer, eval_workers):
    """Example of a custom evaluation function.

    Args:
        trainer (Trainer): trainer class to evaluate.
        eval_workers (WorkerSet): evaluation workers.

    Returns:
        metrics (dict): evaluation metrics dict.
    """

    # We configured 2 eval workers in the training config.
    worker_1, worker_2 = eval_workers.remote_workers()

    # Set different env settings for each worker. Here we use a fixed config,
    # which also could have been computed in each worker by looking at
    # env_config.worker_index (printed in SimpleCorridor class above).
    worker_1.foreach_env.remote(lambda env: env.set_corridor_length(4))
    worker_2.foreach_env.remote(lambda env: env.set_corridor_length(7))

    for i in range(5):
        print("Custom evaluation round", i)
        # Calling .sample() runs exactly one episode per worker due to how the
        # eval workers are configured.
        ray.get([w.sample.remote() for w in eval_workers.remote_workers()])

    # Collect the accumulated episodes on the workers, and then summarize the
    # episode stats into a metrics dict.
    episodes, _ = collect_episodes(
        remote_workers=eval_workers.remote_workers(), timeout_seconds=99999)
    # You can compute metrics from the episodes manually, or use the
    # convenient `summarize_episodes()` utility:
    metrics = summarize_episodes(episodes)
    # Note that the above two statements are the equivalent of:
    # metrics = collect_metrics(eval_workers.local_worker(),
    #                           eval_workers.remote_workers())

    # You can also put custom values in the metrics dict.
    metrics["foo"] = 1
    return metrics


if __name__ == "__main__":
    args = parser.parse_args()

    if args.no_custom_eval:
        eval_fn = None
    else:
        eval_fn = custom_eval_function

    ray.init(num_cpus=args.num_cpus or None)

    config = {
        "env": SimpleCorridor,
        "env_config": {
            "corridor_length": 10,
        },
        "horizon": 20,

        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),

        # Training rollouts will be collected using just the learner
        # process, but evaluation will be done in parallel with two
        # workers. Hence, this run will use 3 CPUs total (1 for the
        # learner + 2 more for evaluation workers).
        "num_workers": 0,
        "evaluation_num_workers": 2,

        # Optional custom eval function.
        "custom_eval_function": eval_fn,

        # Enable evaluation, once per training iteration.
        "evaluation_interval": 1,

        # Run 10 episodes each time evaluation runs.
        "evaluation_duration": 10,

        # Override the env config for evaluation.
        "evaluation_config": {
            "env_config": {
                # Evaluate using LONGER corridor than trained on.
                "corridor_length": 5,
            },
        },
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run("PG", config=config, stop=stop, verbose=1)

    # Check eval results (from eval workers using the custom function),
    # not results from the regular workers.
    if args.as_test:
        check_learning_achieved(results, args.stop_reward, evaluation=True)
    ray.shutdown()
