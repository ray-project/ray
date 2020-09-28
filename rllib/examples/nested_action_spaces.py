import argparse
from gym.spaces import Dict, Tuple, Box, Discrete
import os

import ray
import ray.tune as tune
from ray.tune.registry import register_env
from ray.rllib.examples.env.nested_space_repeat_after_me_env import \
    NestedSpaceRepeatAfterMeEnv
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--torch", action="store_true")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-reward", type=float, default=0.0)
parser.add_argument("--stop-iters", type=int, default=100)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--num-cpus", type=int, default=0)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    register_env("NestedSpaceRepeatAfterMeEnv",
                 lambda c: NestedSpaceRepeatAfterMeEnv(c))

    config = {
        "env": "NestedSpaceRepeatAfterMeEnv",
        "env_config": {
            "space": Dict({
                "a": Tuple(
                    [Dict({
                        "d": Box(-10.0, 10.0, ()),
                        "e": Discrete(2)
                    })]),
                "b": Box(-10.0, 10.0, (2, )),
                "c": Discrete(4)
            }),
        },
        "entropy_coeff": 0.00005,  # We don't want high entropy in this Env.
        "gamma": 0.0,  # No history in Env (bandit problem).
        "lr": 0.0005,
        "num_envs_per_worker": 20,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_sgd_iter": 4,
        "num_workers": 0,
        "vf_loss_coeff": 0.01,
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=1)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
