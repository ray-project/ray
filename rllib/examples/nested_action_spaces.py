import argparse
import gym
from gym.spaces import Dict, Tuple, Box, Discrete
import numpy as np
import sys

import ray
from ray.tune.registry import register_env
from ray.rllib.examples.env.nested_space_repeat_after_me_env import \
    NestedSpaceRepeatAfterMeEnv
from ray.rllib.utils import try_import_tree
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.space_utils import flatten_space

tf = try_import_tf()
tree = try_import_tree()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--torch", action="store_true")
parser.add_argument("--stop", type=int, default=90)
parser.add_argument("--max-trainstop", type=int, default=90)
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
        "lr": 0.0003,
        "num_envs_per_worker": 20,
        "num_sgd_iter": 20,
        "num_workers": 0,
        "use_pytorch": args.torch,
        "vf_loss_coeff": 0.01,
    }

    import ray.rllib.agents.ppo as ppo
    trainer = ppo.PPOTrainer(config=config)
    for _ in range(100):
        results = trainer.train()
        print(results)
        if results["episode_reward_mean"] > args.stop:
            sys.exit(0)  # Learnt, exit gracefully.
    sys.exit(1)  # Done, but did not learn, exit with error.
