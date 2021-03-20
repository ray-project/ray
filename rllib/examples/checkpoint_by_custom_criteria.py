import argparse
import os

import ray
from ray import tune

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=150.0)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    # Simple PPO config.
    config = {
        "env": "CartPole-v0",
        # Run 3 trials.
        "lr": tune.grid_search([0.01, 0.001, 0.0001]),
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
        # Run with tracing enabled for tfe/tf2.
        "eager_tracing": args.framework in ["tfe", "tf2"],
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Run tune for some iterations and generate checkpoints.
    results = tune.run(args.run, config=config, stop=stop, checkpoint_freq=1)

    # Get the best of the 3 trials by using some metric.
    # NOTE: Choosing the min `episodes_this_iter` automatically picks the trial
    # with the best performance (over the entire run (scope="all")):
    # The fewer episodes, the longer each episode lasted, the more reward we
    # got each episode.
    # Setting scope to "last", "last-5-avg", or "last-10-avg" will only compare
    # (using `mode=min|max`) the average values of the last 1, 5, or 10
    # iterations with each other, respectively.
    # Setting scope to "avg" will compare (using `mode`=min|max) the average
    # values over the entire run.
    metric = "episodes_this_iter"
    best_trial = results.get_best_trial(metric=metric, mode="min", scope="all")
    value_best_metric = best_trial.metric_analysis[metric]["min"]
    print("Best trial's lowest episode length (over all "
          "iterations): {}".format(value_best_metric))

    # Confirm, we picked the right trial.
    assert all(value_best_metric <= results.results[t][metric]
               for t in results.results.keys())

    # Get the best checkpoints from the trial, based on different metrics.
    # Checkpoint with the lowest policy loss value:
    ckpt = results.get_best_checkpoint(
        best_trial,
        metric="info/learner/default_policy/learner_stats/policy_loss",
        mode="min")
    print("Lowest pol-loss: {}".format(ckpt))

    # Checkpoint with the highest value-function loss:
    ckpt = results.get_best_checkpoint(
        best_trial,
        metric="info/learner/default_policy/learner_stats/vf_loss",
        mode="max")
    print("Highest vf-loss: {}".format(ckpt))

    ray.shutdown()
