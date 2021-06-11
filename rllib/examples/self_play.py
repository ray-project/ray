"""Example showing how one can implement a simple self-play training workflow.

Checks for training progress each training iteration, adds a new policy to
the policy map (frozen copy of the current one), changes the policy_mapping_fn
to make new matches, and edits the policies_to_train list.
"""

import argparse
from gym.spaces import Dict, MultiDiscrete, Tuple
import os
import pyspiel

import ray
from ray import tune
from ray.tune import register_env
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="PG",
    help="The RLlib-registered algorithm to use.")
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
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
    default=50000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=7.0,
    help="Reward at which we stop training.")

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)

    dummy_env = OpenSpielEnv(pyspiel.load_game("connect_four"))
    obs = dummy_env.reset()
    obs, reward, done, _ = dummy_env.step({0: 4})

    obs_space = Tuple([
        Dict({
            "obs": MultiDiscrete([2, 2, 2, 3]),
            ENV_STATE: MultiDiscrete([2, 2, 2])
        }),
        Dict({
            "obs": MultiDiscrete([2, 2, 2, 3]),
            ENV_STATE: MultiDiscrete([2, 2, 2])
        }),
    ])
    act_space = Tuple([
        TwoStepGame.action_space,
        TwoStepGame.action_space,
    ])
    register_env(
        "connect_four",
        lambda _: OpenSpielEnv(pyspiel.load_game("connect_four")))

    config = {
        "env": "connect_four",
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
    }

    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    results = tune.run(args.run, stop=stop, config=config, verbose=1)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
