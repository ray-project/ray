"""Example showing how one can implement a simple self-play training workflow.

Checks for training progress each training iteration, adds a new policy to
the policy map (frozen copy of the current one), changes the policy_mapping_fn
to make new matches, and edits the policies_to_train list.
"""

import argparse
import os
import pyspiel

import ray
from ray import tune
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.env.wrappers.open_spiel import OpenSpielEnv
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune import register_env

class SelfPlayCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        # 0=RandomPolicy, 1=1st main policy snapshot,
        # 2=2nd main policy snapshot, etc..
        self.current_opponent = 0

    def on_train_result(self, *, trainer, result, **kwargs):
        # Get the win rate:
        won = 0
        for r_main in result["hist_stats"]["policy_main_reward"]:
            if r_main > 0.0:
                won += 1
        win_rate = won / len(result["hist_stats"]["policy_main_reward"])
        # If win rate is good -> Store current policy and play against
        # it next.
        if win_rate > 0.75:
            self.current_opponent += 1
            new_policy = trainer.add_policy(
                policy_id=f"main_{self.current_opponent}",
                observatio_space=
            )
            trainer.workers.for


parser = argparse.ArgumentParser()
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

    ray.init(num_cpus=args.num_cpus or None, local_mode=True)#TODO

    dummy_env = OpenSpielEnv(pyspiel.load_game("connect_four"))
    obs_space = dummy_env.observation_space
    action_space = dummy_env.action_space
    obs = dummy_env.reset()
    obs, rewards, dones, _ = dummy_env.step({0: 4})

    register_env(
        "connect_four",
        lambda _: OpenSpielEnv(pyspiel.load_game("connect_four")))

    config = {
        "env": "connect_four",
        "num_workers": 0, #TODOremove
        "callbacks": SelfPlayCallback,
        "multiagent": {
            # Initial policy map: Random and PPO.
            "policies": {
                "main": (None, obs_space, action_space, {}),
                "opponent": (RandomPolicy, obs_space, action_space, {}),
            },
            "policy_mapping_fn": lambda agent_id: "main" if agent_id == 0 else "opponent",
            "policies_to_train": ["main"],
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
    }

    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    results = tune.run("PPO", stop=stop, config=config, verbose=1)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
