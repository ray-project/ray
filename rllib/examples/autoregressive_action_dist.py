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
from ray.rllib.examples.models.autoregressive_action_model import \
    AutoregressiveActionModel, TorchAutoregressiveActionModel
from ray.rllib.examples.models.autoregressive_action_dist import \
    BinaryAutoregressiveDistribution, TorchBinaryAutoregressiveDistribution
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")  # try PG, PPO, IMPALA
parser.add_argument("--torch", action="store_true")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=200)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None)
    ModelCatalog.register_custom_model(
        "autoregressive_model", TorchAutoregressiveActionModel
        if args.torch else AutoregressiveActionModel)
    ModelCatalog.register_custom_action_dist(
        "binary_autoreg_dist", TorchBinaryAutoregressiveDistribution
        if args.torch else BinaryAutoregressiveDistribution)

    config = {
        "env": CorrelatedActionsEnv,
        "gamma": 0.5,
        "num_gpus": 0,
        "model": {
            "custom_model": "autoregressive_model",
            "custom_action_dist": "binary_autoreg_dist",
        },
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, stop=stop, config=config)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
