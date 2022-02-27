"""
This script demonstrates how one can specify n (vectorized) envs
as ray remote (actors), such that stepping through these occurs in parallel.
Also, actions for each env step will be calculated on the "main" node.

This can be useful if the "main" node is a GPU machine and we would like to
speed up batched action calculations, similar to DeepMind's SEED
architecture, described here:

https://ai.googleblog.com/2020/03/massively-scaling-reinforcement.html
"""
import argparse
import os

import ray
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.agents.trainer import Trainer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.test_utils import check_learning_achieved
from ray import tune
from ray.tune import PlacementGroupFactory
from ray.tune.logger import pretty_print


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # example-specific args
    # This should be >1, otherwise, remote envs make no sense.
    parser.add_argument("--num-envs-per-worker", type=int, default=4)

    # general args
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "tfe", "torch"],
        default="tf",
        help="The DL framework specifier.",
    )
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
        "--stop-timesteps",
        type=int,
        default=100000,
        help="Number of timesteps to train.",
    )
    parser.add_argument(
        "--stop-reward",
        type=float,
        default=150.0,
        help="Reward at which we stop training.",
    )
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Run without Tune using a manual train loop instead. Here,"
        "there is no TensorBoard support.",
    )
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.",
    )

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


# The modified Trainer class we will use. This is the exact same
# as a PPOTrainer, but with the additional default_resource_request
# override, telling tune that it's ok (not mandatory) to place our
# n remote envs on a different node (each env using 1 CPU).
class PPOTrainerRemoteInference(PPOTrainer):
    @classmethod
    @override(Trainer)
    def default_resource_request(cls, config):
        cf = dict(cls.get_default_config(), **config)

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[
                {
                    # Single CPU for the local worker. This CPU will host the
                    # main model in this example (num_workers=0).
                    "CPU": 1,
                    # Possibly add n GPUs to this.
                    "GPU": cf["num_gpus"],
                },
                {
                    # Different bundle (meaning: possibly different node)
                    # for your n "remote" envs (set remote_worker_envs=True).
                    "CPU": cf["num_envs_per_worker"],
                },
            ],
            strategy=config.get("placement_strategy", "PACK"),
        )


if __name__ == "__main__":
    args = get_cli_args()

    ray.init(num_cpus=6, local_mode=args.local_mode)

    config = {
        "env": "CartPole-v0",
        # Force sub-envs to be ray.actor.ActorHandles, so we can step
        # through them in parallel.
        "remote_worker_envs": True,
        # Set the number of CPUs used by the (local) worker, aka "driver"
        # to match the number of ray remote envs.
        "num_cpus_for_driver": args.num_envs_per_worker + 1,
        # Use a single worker (however, with n parallelized remote envs, maybe
        # even running on another node).
        # Action computations will occur on the "main" (GPU?) node, while
        # the envs run on one or more CPU node(s).
        "num_workers": 0,
        "num_envs_per_worker": args.num_envs_per_worker,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
    }

    # Run as manual training loop.
    if args.no_tune:
        # manual training loop using PPO and manually keeping track of state
        trainer = PPOTrainerRemoteInference(config=config)
        # run manual training loop and print results after each iteration
        for _ in range(args.stop_iters):
            result = trainer.train()
            print(pretty_print(result))
            # Stop training if the target train steps or reward are reached.
            if (
                result["timesteps_total"] >= args.stop_timesteps
                or result["episode_reward_mean"] >= args.stop_reward
            ):
                break

    # Run with Tune for auto env and trainer creation and TensorBoard.
    else:
        stop = {
            "training_iteration": args.stop_iters,
            "timesteps_total": args.stop_timesteps,
            "episode_reward_mean": args.stop_reward,
        }

        results = tune.run(
            PPOTrainerRemoteInference, config=config, stop=stop, verbose=1
        )

        if args.as_test:
            check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
