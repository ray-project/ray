import argparse
import gymnasium as gym
from gymnasium.spaces import Dict, Tuple, Box, Discrete
import os

import ray
from ray import air, tune
from ray.rllib.examples.env.nested_space_repeat_after_me_env import (
    NestedSpaceRepeatAfterMeEnv,
)
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.",
)
parser.add_argument(
    "--stop-iters", type=int, default=100, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=0.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)
    gym.register(
        "NestedSpaceRepeatAfterMeEnv", lambda c: NestedSpaceRepeatAfterMeEnv(c)
    )

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment(
            "NestedSpaceRepeatAfterMeEnv",
            env_config={
                "space": Dict(
                    {
                        "a": Tuple(
                            [Dict({"d": Box(-10.0, 10.0, ()), "e": Discrete(2)})]
                        ),
                        "b": Box(-10.0, 10.0, (2,)),
                        "c": Discrete(4),
                    }
                ),
            },
        )
        .framework(args.framework)
        .rollouts(num_rollout_workers=0, num_envs_per_worker=20)
        # No history in Env (bandit problem).
        .training(gamma=0.0, lr=0.0005)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    if args.run == "PPO":
        config.training(
            # We don't want high entropy in this Env.
            entropy_coeff=0.00005,
            num_sgd_iter=4,
            vf_loss_coeff=0.01,
        )

    stop = {
        "training_iteration": args.stop_iters,
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    results = tune.Tuner(
        args.run, param_space=config, run_config=air.RunConfig(stop=stop, verbose=1)
    ).fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
