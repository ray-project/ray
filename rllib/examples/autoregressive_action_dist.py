"""
Example of specifying an autoregressive action distribution.

In an action space with multiple components (e.g., Tuple(a1, a2)), you might
want a2 to be sampled based on the sampled value of a1, i.e.,
a2_sampled ~ P(a2 | a1_sampled, obs). Normally, a1 and a2 would be sampled
independently.

To do this, you need both a custom model that implements the autoregressive
pattern, and a custom action distribution class that leverages that model.
This examples shows both.
---
The example uses the CorrelatedActionsEnv where the agent observes a random
number (0 or 1) and has to choose two actions a1 and a2.
Action a1 should match the observation and a2 should match a1.

# TODO: why does it not get the requested resources with multiple cpus?
# TODO: better understand action dist and model
# TODO: add usage description of example
"""

import argparse
import os

import ray
from ray import tune
from ray.rllib.agents import ppo
from ray.rllib.examples.env.correlated_actions_env import CorrelatedActionsEnv
from ray.rllib.examples.models.autoregressive_action_model import \
    AutoregressiveActionModel, TorchAutoregressiveActionModel
from ray.rllib.examples.models.autoregressive_action_dist import \
    BinaryAutoregressiveDistribution, TorchBinaryAutoregressiveDistribution
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.logger import pretty_print


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # example-specific args
    # TODO: extra option: --no-correlation --> slower to learn?

    # general args
    parser.add_argument(
        "--run",
        type=str,
        default="PPO",
        help="The RLlib-registered algorithm to use.")
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "tfe", "torch"],
        default="tf",
        help="The DL framework specifier.")
    # FIXME: why does it get stuck if I set cpu to 2?
    parser.add_argument("--num-cpus", type=int, default=0)
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
        default=200.0,
        help="Reward at which we stop training.")
    parser.add_argument(
        "--no-tune",
        action="store_true",
        help="Run without Tune using a manual train loop instead. Here,"
        "there is no TensorBoard support.")
    parser.add_argument(
        "--local-mode",
        action="store_true",
        help="Init Ray in local mode for easier debugging.")

    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()
    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    # main part: register and configure autoregressive action model and dist
    ModelCatalog.register_custom_model(
        "autoregressive_model", TorchAutoregressiveActionModel
        if args.framework == "torch" else AutoregressiveActionModel)
    ModelCatalog.register_custom_action_dist(
        "binary_autoreg_dist", TorchBinaryAutoregressiveDistribution
        if args.framework == "torch" else BinaryAutoregressiveDistribution)

    config = {
        "env": CorrelatedActionsEnv,
        "gamma": 0.5,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "model": {
            "custom_model": "autoregressive_model",
            "custom_action_dist": "binary_autoreg_dist",
        },
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # training loop
    # manual training loop using PPO and manually keeping track of state
    if args.no_tune:
        if args.run != "PPO":
            raise ValueError("Only support --run PPO with --no-tune.")
        ppo_config = ppo.DEFAULT_CONFIG.copy()
        ppo_config.update(config)
        trainer = ppo.PPOTrainer(config=ppo_config, env=args.env)
        # run manual training loop and print results after each iteration
        for _ in range(args.stop_iters):
            result = trainer.train()
            print(pretty_print(result))
            # stop training if the target train steps or reward are reached
            if result["timesteps_total"] >= args.stop_timesteps or \
                    result["episode_reward_mean"] >= args.stop_reward:
                break

        # TODO: implement manual test loop

    # run with Tune for auto env and trainer creation and TensorBoard
    else:
        results = tune.run(args.run, stop=stop, config=config, verbose=2)
        print(results)

        if args.as_test:
            print("Checking if learning goals were achieved")
            check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
