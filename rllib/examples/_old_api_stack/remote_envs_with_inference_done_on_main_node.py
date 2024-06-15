# @OldAPIStack
"""
This script specifies n (vectorized) envs
as Ray remote (actors), such that stepping through these occurs in parallel.
Also, actions for each env step are calculated on the "main" node.

This behavior can be useful if the "main" node is a GPU machine and you would like to
speed up batched action calculations, similar to DeepMind's SEED
architecture, described here:

https://ai.googleblog.com/2020/03/massively-scaling-reinforcement.html
"""
import argparse
import os
from typing import Union

import ray
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.ppo import PPO, PPOConfig
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.rllib.utils.typing import PartialAlgorithmConfigDict
from ray.tune import PlacementGroupFactory
from ray.tune.logger import pretty_print


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # example-specific args
    # This should be >1, otherwise, remote envs make no sense.
    parser.add_argument("--num-envs-per-env-runner", type=int, default=4)

    # general args
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="torch",
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


# The modified Algorithm class we use:
# Subclassing from PPO, our algo only modifies `default_resource_request`,
# telling Ray Tune that it's ok (not mandatory) to place our n remote envs on a
# different node (each env using 1 CPU).
class PPORemoteInference(PPO):
    @classmethod
    @override(Algorithm)
    def default_resource_request(
        cls,
        config: Union[AlgorithmConfig, PartialAlgorithmConfigDict],
    ):
        if isinstance(config, AlgorithmConfig):
            cf = config
        else:
            cf = cls.get_default_config().update_from_dict(config)

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[
                {
                    # Single CPU for the local worker. This CPU hosts the
                    # main model in this example (num_env_runners=0).
                    "CPU": 1,
                    # Possibly add n GPUs to this.
                    "GPU": cf.num_gpus,
                },
                {
                    # Different bundle (meaning: possibly different node)
                    # for your n "remote" envs (set remote_worker_envs=True).
                    "CPU": cf.num_envs_per_env_runner,
                },
            ],
            strategy=cf.placement_strategy,
        )


if __name__ == "__main__":
    args = get_cli_args()

    ray.init(num_cpus=6, local_mode=args.local_mode)

    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .framework(args.framework)
        .env_runners(
            # Force sub-envs to be ray.actor.ActorHandles, so we can step
            # through them in parallel.
            remote_worker_envs=True,
            num_envs_per_env_runner=args.num_envs_per_env_runner,
            # Use a single worker (however, with n parallelized remote envs, maybe
            # even running on another node).
            # Action computations occur on the "main" (GPU?) node, while
            # the envs run on one or more CPU node(s).
            num_env_runners=0,
        )
        .resources(
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            # Set the number of CPUs used by the (local) worker, aka "driver"
            # to match the number of Ray remote envs.
            num_cpus_for_main_process=args.num_envs_per_env_runner + 1,
        )
    )

    # Run as manual training loop.
    if args.no_tune:
        # manual training loop using PPO and manually keeping track of state
        algo = PPORemoteInference(config=config)
        # run manual training loop and print results after each iteration
        for _ in range(args.stop_iters):
            result = algo.train()
            print(pretty_print(result))
            # Stop training if the target train steps or reward are reached.
            if (
                result[f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}"] >= args.stop_timesteps
                or result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= args.stop_reward
            ):
                break

    # Run with Tune for auto env and algorithm creation and TensorBoard.
    else:
        stop = {
            TRAINING_ITERATION: args.stop_iters,
            NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
            f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
        }

        results = tune.Tuner(
            PPORemoteInference,
            param_space=config,
            run_config=air.RunConfig(stop=stop, verbose=1),
        ).fit()

        if args.as_test:
            check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
