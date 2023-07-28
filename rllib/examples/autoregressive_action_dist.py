"""
Example of specifying an autoregressive action distribution.

In an action space with multiple components (e.g., Tuple(a1, a2)), you might
want a2 to be sampled based on the sampled value of a1, i.e.,
a2_sampled ~ P(a2 | a1_sampled, obs). Normally, a1 and a2 would be sampled
independently.

To do this, you need both a custom model that implements the autoregressive
pattern, and a custom action distribution class that leverages that model.
This examples shows both.

Related paper: https://arxiv.org/abs/1903.11524

The example uses the CorrelatedActionsEnv where the agent observes a random
number (0 or 1) and has to choose two actions a1 and a2.
Action a1 should match the observation (+5 reward) and a2 should match a1
(+5 reward).
Since a2 should depend on a1, an autoregressive action dist makes sense.

---
To better understand the environment, run 1 manual train iteration and test
loop without Tune:
$ python autoregressive_action_dist.py --stop-iters 1 --no-tune

Run this example with defaults (using Tune and autoregressive action dist):
$ python autoregressive_action_dist.py
Then run again without autoregressive actions:
$ python autoregressive_action_dist.py --no-autoreg
# TODO: Why does this lead to better results than autoregressive actions?
Compare learning curve on TensorBoard:
$ cd ~/ray-results/; tensorboard --logdir .

Other options for running this example:
$ python attention_net.py --help
"""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.examples.env.correlated_actions_env import CorrelatedActionsEnv
from ray.rllib.examples.models.autoregressive_action_model import (
    AutoregressiveActionModel,
    TorchAutoregressiveActionModel,
)
from ray.rllib.examples.models.autoregressive_action_dist import (
    BinaryAutoregressiveDistribution,
    TorchBinaryAutoregressiveDistribution,
)
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.logger import pretty_print
from ray.tune.registry import get_trainable_cls


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()

    # example-specific arg: disable autoregressive action dist
    parser.add_argument(
        "--no-autoreg",
        action="store_true",
        help="Do NOT use an autoregressive action distribution but normal,"
        "independently distributed actions.",
    )

    # general args
    parser.add_argument(
        "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
    )
    parser.add_argument(
        "--framework",
        choices=["tf", "tf2", "torch"],
        default="torch",
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
        "--stop-iters", type=int, default=200, help="Number of iterations to train."
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
        default=200.0,
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


if __name__ == "__main__":
    args = get_cli_args()
    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    # main part: register and configure autoregressive action model and dist
    # here, tailored to the CorrelatedActionsEnv such that a2 depends on a1
    ModelCatalog.register_custom_model(
        "autoregressive_model",
        TorchAutoregressiveActionModel
        if args.framework == "torch"
        else AutoregressiveActionModel,
    )
    ModelCatalog.register_custom_action_dist(
        "binary_autoreg_dist",
        TorchBinaryAutoregressiveDistribution
        if args.framework == "torch"
        else BinaryAutoregressiveDistribution,
    )

    # Generic config.
    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment(CorrelatedActionsEnv)
        .framework(args.framework)
        .training(gamma=0.5)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        # Batch-norm models have not been migrated to the RL Module API yet.
        .training(_enable_learner_api=False)
        .rl_module(_enable_rl_module_api=False)
    )

    # Use registered model and dist in config.
    if not args.no_autoreg:
        config.model.update(
            {
                "custom_model": "autoregressive_model",
                "custom_action_dist": "binary_autoreg_dist",
            }
        )

    # use stop conditions passed via CLI (or defaults)
    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # manual training loop using PPO without ``Tuner.fit()``.
    if args.no_tune:
        if args.run != "PPO":
            raise ValueError("Only support --run PPO with --no-tune.")
        # Have to specify this here are we are working with a generic AlgorithmConfig
        # object, not a specific one (e.g. PPOConfig).
        config.algo_class = args.run
        algo = config.build()
        # run manual training loop and print results after each iteration
        for _ in range(args.stop_iters):
            result = algo.train()
            print(pretty_print(result))
            # stop training if the target train steps or reward are reached
            if (
                result["timesteps_total"] >= args.stop_timesteps
                or result["episode_reward_mean"] >= args.stop_reward
            ):
                break

        # run manual test loop: 1 iteration until done
        print("Finished training. Running manual test/inference loop.")
        env = CorrelatedActionsEnv(_)
        obs, info = env.reset()
        done = False
        total_reward = 0
        while not done:
            a1, a2 = algo.compute_single_action(obs)
            next_obs, reward, done, truncated, _ = env.step((a1, a2))
            print(f"Obs: {obs}, Action: a1={a1} a2={a2}, Reward: {reward}")
            obs = next_obs
            total_reward += reward
        print(f"Total reward in test episode: {total_reward}")
        algo.stop()

    # run with Tune for auto env and Algorithm creation and TensorBoard
    else:
        tuner = tune.Tuner(
            args.run, run_config=air.RunConfig(stop=stop, verbose=2), param_space=config
        )
        results = tuner.fit()

        if args.as_test:
            print("Checking if learning goals were achieved")
            check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
