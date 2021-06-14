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
from ray.tune import register_env

OBS_SPACE = ACTION_SPACE = None

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--stop-iters",
    type=int,
    default=20000,
    help="Number of iterations to train.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=1000000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--win-rate-threshold",
    type=float,
    default=0.85,
    help="Win-rate at which we setup another opponent by freezing the "
    "current main policy and playing against it from here on.")
args = parser.parse_args()


class SelfPlayCallback(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        # 0=RandomPolicy, 1=1st main policy snapshot,
        # 2=2nd main policy snapshot, etc..
        self.current_opponent = 0

    def on_train_result(self, *, trainer, result, **kwargs):
        # Get the win rate for the train batch.
        # Note that normally, one should set up a proper evaluation config,
        # such that evaluation always happens on the already updated policy,
        # instead of on the already used train_batch.
        opponent_rew = result["hist_stats"]["policy_{}_reward".format(
            "main_" + str(self.current_opponent)
            if self.current_opponent > 0 else "random")]
        won = 0
        for r_main, r_opponent in zip(
                result["hist_stats"]["policy_main_reward"], opponent_rew):
            if r_main > r_opponent:
                won += 1
        win_rate = won / len(result["hist_stats"]["policy_main_reward"])
        print(f"Win rate={win_rate} -> ", end="")
        # If win rate is good -> Snapshot current policy and play against
        # it next, keeping the snapshot fixed and only improving the "main"
        # policy.
        if win_rate > args.win_rate_threshold:
            self.current_opponent += 1
            new_pol_id = f"main_{self.current_opponent}"

            def policy_mapping_fn(*, agent_id, episode, **kwargs):
                # agent_id = [0|1] -> policy depends on episode ID
                # This way, we make sure that both policies sometimes play
                # agent0 (start player) and sometimes agent1.
                return "main" if episode.episode_id % 2 == agent_id \
                    else new_pol_id

            new_policy = trainer.add_policy(
                policy_id=new_pol_id,
                policy_cls=type(trainer.get_policy("main")),
                observation_space=OBS_SPACE,
                action_space=ACTION_SPACE,
                config={},
                policy_mapping_fn=policy_mapping_fn,
            )
            # Set the weights of the new policy to the main policy.
            # We'll keep training the main policy, whereas `new_pol_id` will
            # remain fixed.
            new_policy.set_state(trainer.get_policy("main").get_state())
            print("Play against better opponent next.")
        else:
            print("Not good enough, keep learning.")


if __name__ == "__main__":
    ray.init(num_cpus=args.num_cpus or None)

    dummy_env = OpenSpielEnv(pyspiel.load_game("connect_four"))
    OBS_SPACE = dummy_env.observation_space
    ACTION_SPACE = dummy_env.action_space

    register_env("connect_four",
                 lambda _: OpenSpielEnv(pyspiel.load_game("connect_four")))

    def policy_mapping_fn(*, agent_id, episode, **kwargs):
        # agent_id = [0|1] -> policy depends on episode ID
        # This way, we make sure that both policies sometimes play agent0
        # (start player) and sometimes agent1.
        return "main" if episode.episode_id % 2 == agent_id else "random"

    config = {
        "env": "connect_four",
        "callbacks": SelfPlayCallback,
        "num_workers": 2,
        "multiagent": {
            # Initial policy map: Random and PPO. This will be expanded
            # to more policy snapshots taken from "main" against which "main"
            # will then play (instead of "random"). This is done in the
            # custom callback defined above (`SelfPlayCallback`).
            "policies": {
                # Our main policy, we'd like to optimize.
                "main": (None, OBS_SPACE, ACTION_SPACE, {}),
                # An initial random opponent to play against.
                "random": (RandomPolicy, OBS_SPACE, ACTION_SPACE, {}),
            },
            # Assign agent 0 and 1 randomly to the "main" policy or
            # to the opponent ("random" at first). Make sure (via episode_id)
            # that "main" always plays against "random" (and not against
            # another "main).
            "policy_mapping_fn": policy_mapping_fn,
            # Always just train the "main" policy.
            "policies_to_train": ["main"],
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
    }

    stop = {
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    results = tune.run("PPO", stop=stop, config=config, verbose=1)

    ray.shutdown()
