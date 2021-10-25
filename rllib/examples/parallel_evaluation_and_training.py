import argparse
import os

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()

parser.add_argument(
    "--evaluation-num-episodes",
    type=lambda v: v if v == "auto" else int(v),
    default=13,
    help="Number of evaluation episodes to run each iteration. "
    "If 'auto', will run as many as possible during train pass.")

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
    default=200000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=180.0,
    help="Reward at which we stop training.")
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.")


class AssertNumEvalEpisodesCallback(DefaultCallbacks):
    def on_train_result(self, *, trainer, result, **kwargs):
        # Make sure we always run exactly n evaluation episodes,
        # no matter what the other settings are (such as
        # `evaluation_num_workers` or `evaluation_parallel_to_training`).
        if "evaluation" in result:
            hist_stats = result["evaluation"]["hist_stats"]
            num_episodes_done = len(hist_stats["episode_lengths"])
            # Compare number of entries in episode_lengths (this is the
            # number of episodes actually run) with desired number of
            # episodes from the config.
            if isinstance(trainer.config["evaluation_num_episodes"], int):
                assert num_episodes_done == \
                    trainer.config["evaluation_num_episodes"]
            else:
                assert trainer.config["evaluation_num_episodes"] == "auto"
                assert num_episodes_done >= \
                       trainer.config["evaluation_num_workers"]
            print("Number of run evaluation episodes: "
                  f"{num_episodes_done} (ok)!")


if __name__ == "__main__":
    import ray
    from ray import tune

    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    config = {
        "env": "CartPole-v0",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
        # Run with tracing enabled for tfe/tf2.
        "eager_tracing": args.framework in ["tfe", "tf2"],

        # Parallel evaluation+training config.
        # Switch on evaluation in parallel with training.
        "evaluation_parallel_to_training": True,
        # Use two evaluation workers. Must be >0, otherwise,
        # evaluation will run on a local worker and block (no parallelism).
        "evaluation_num_workers": 2,
        # Evaluate every other training iteration (together
        # with every other call to Trainer.train()).
        "evaluation_interval": 2,
        # Run for n episodes (properly distribute load amongst all eval
        # workers). The longer it takes to evaluate, the more
        # sense it makes to use `evaluation_parallel_to_training=True`.
        # Use "auto" to run evaluation for roughly as long as the training
        # step takes.
        "evaluation_num_episodes": args.evaluation_num_episodes,

        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation OR - in auto
        # mode - run at least as many episodes as we have eval workers.
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
