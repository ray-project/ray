import argparse
import os

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="PPO",
    help="The RLlib-registered algorithm to use.")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.")
parser.add_argument(
    "--stop-iters",
    type=int,
    default=200,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=100000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=100.0,
    help="Reward at which we stop training.")


class AssertNumEvalEpisodesCallback(DefaultCallbacks):
    def on_train_result(self, *, trainer, result, **kwargs):
        # Make sure we always run exactly n evaluation episodes,
        # no matter what the other settings are (such as
        # `evaluation_num_workers` or `evaluation_parallel_to_training`).
        if "evaluation" in result:
            hist_stats = result["evaluation"]["hist_stats"]
            assert len(hist_stats["episode_lengths"]) == \
                trainer.config["evaluation_num_episodes"]
            print("Number of evaluation episodes is exactly "
                  f"{trainer.config['evaluation_num_episodes']} (ok)!")


if __name__ == "__main__":
    import ray
    from ray import tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    config = {
        "env": "CartPole-v0",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "model": {
            "vf_share_layers": True,
        },
        "framework": args.framework,
        # Run with tracing enabled for tfe/tf2.
        "eager_tracing": args.framework in ["tfe", "tf2"],
        # Parallel evaluation+training config.
        # Use two evaluation workers (must be >0, otherwise,
        # evaluation will run on a local worker and block).
        "evaluation_num_workers": 2,
        # Evaluate every other training iteration (together
        # with every other call to Trainer.train()).
        "evaluation_interval": 2,
        # Run for 50 episodes (25 per eval worker and per
        # evaluation round). The longer it takes to evaluate, the more
        # sense it makes to use `evaluation_parallel_to_training=True`.
        "evaluation_num_episodes": 13,
        # Switch on evaluation in parallel with training.
        "evaluation_parallel_to_training": True,

        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation.
        "callbacks": AssertNumEvalEpisodesCallback,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=2)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
