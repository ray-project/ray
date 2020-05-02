"""Example of specifying an autoregressive action distribution.

In an action space with multiple components (e.g., Tuple(a1, a2)), you might
want a2 to be sampled based on the sampled value of a1, i.e.,
a2_sampled ~ P(a2 | a1_sampled, obs). Normally, a1 and a2 would be sampled
independently.

To do this, you need both a custom model that implements the autoregressive
pattern, and a custom action distribution class that leverages that model.
This examples shows both.
"""

import argparse

import ray
from ray import tune
from ray.rllib.examples.env.correlated_actions_env import CorrelatedActionsEnv
from ray.rllib.models import ModelCatalog

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")  # try PG, PPO, IMPALA
parser.add_argument("--stop", type=int, default=200)
parser.add_argument("--num-cpus", type=int, default=0)


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    ModelCatalog.register_custom_model(
        "autoregressive_model", AutoregressiveActionsModel)
    ModelCatalog.register_custom_action_dist(
        "binary_autoreg_dist", BinaryAutoregressiveDistribution if
        args.torch else TorchBinaryAutoregressiveDistribution)
    tune.run(
        args.run,
        stop={"episode_reward_mean": args.stop},
        config={
            "env": CorrelatedActionsEnv,
            "gamma": 0.5,
            "num_gpus": 0,
            "model": {
                "custom_model": "autoregressive_model",
                "custom_action_dist": "binary_autoreg_dist",
            },
        })
