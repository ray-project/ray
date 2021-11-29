import argparse
import os

from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()

parser.add_argument(
    "--evaluation-duration",
    type=lambda v: v if v == "auto" else int(v),
    default=13,
    help="Number of evaluation episodes/timesteps to run each iteration. "
    "If 'auto', will run as many as possible during train pass.")
parser.add_argument(
    "--evaluation-duration-unit",
    type=str,
    default="episodes",
    choices=["episodes", "timesteps"],
    help="The unit in which to measure the duration (`episodes` or"
    "`timesteps`).")
parser.add_argument(
    "--evaluation-num-workers",
    type=int,
    default=2,
    help="The number of evaluation workers to setup. "
    "0 for a single local evaluation worker. Note that for values >0, no"
    "local evaluation worker will be created (b/c not needed).")
parser.add_argument(
    "--evaluation-interval",
    type=int,
    default=2,
    help="Every how many train iterations should we run an evaluation loop?")

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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Keep track of the number of timesteps that have not been reported
        # yet in the evaluation metrics as they belong to an ongoing episode.
        self.ts_not_reported_yet = 0

    def on_train_result(self, *, trainer, result, **kwargs):
        # Make sure we always run exactly the given evaluation duration,
        # no matter what the other settings are (such as
        # `evaluation_num_workers` or `evaluation_parallel_to_training`).
        if "evaluation" in result:
            hist_stats = result["evaluation"]["hist_stats"]
            # We count in episodes.
            if trainer.config["evaluation_duration_unit"] == "episodes":
                num_episodes_done = len(hist_stats["episode_lengths"])
                # Compare number of entries in episode_lengths (this is the
                # number of episodes actually run) with desired number of
                # episodes from the config.
                if isinstance(trainer.config["evaluation_duration"], int):
                    assert num_episodes_done == \
                        trainer.config["evaluation_duration"]
                # If auto-episodes: Expect at least as many episode as workers
                # (each worker's `sample()` is at least called once).
                else:
                    assert trainer.config["evaluation_duration"] == "auto"
                    assert num_episodes_done >= \
                           trainer.config["evaluation_num_workers"]
                print("Number of run evaluation episodes: "
                      f"{num_episodes_done} (ok)!")
            # We count in timesteps.
            else:
                num_timesteps_reported = sum(hist_stats["episode_lengths"])
                num_timesteps_done = trainer.config["evaluation_duration"]
                if num_timesteps_done != "auto":
                    delta = num_timesteps_done - num_timesteps_reported
                    # Expected.
                    if delta >= 0:
                        self.ts_not_reported_yet += delta
                    else:
                        new_ts_not_reported_yet = \
                            num_timesteps_done - \
                            (num_timesteps_reported - self.overhang)
                        assert new_ts_not_reported_yet >= 0
                        self.ts_not_reported_yet = new_ts_not_reported_yet
                # If auto-timesteps: Expect roughly the same number of
                # timesteps as were done by the normal workers
                # (train_batch_size).
                else:
                    batch_size = trainer.config["train_batch_size"]
                    assert abs(batch_size - num_timesteps_reported) <= 500
                print("Number of run evaluation timesteps: "
                      f"{num_timesteps_reported} (ok)!")

            print(f"R={result['evaluation']['episode_reward_mean']}")


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
        "evaluation_num_workers": args.evaluation_num_workers,
        # Evaluate every other training iteration (together
        # with every other call to Trainer.train()).
        "evaluation_interval": args.evaluation_interval,
        # Run for n episodes/timesteps (properly distribute load amongst
        # all eval workers). The longer it takes to evaluate, the more sense
        # it makes to use `evaluation_parallel_to_training=True`.
        # Use "auto" to run evaluation for roughly as long as the training
        # step takes.
        "evaluation_duration": args.evaluation_duration,
        # "episodes" or "timesteps".
        "evaluation_duration_unit": args.evaluation_duration_unit,

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
