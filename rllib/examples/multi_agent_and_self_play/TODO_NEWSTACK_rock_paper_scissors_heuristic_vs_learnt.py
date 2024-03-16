"""A simple multi-agent env with two agents playing rock paper scissors.

This demonstrates running the following policies in competition:
    (1) heuristic policy of repeating the same move
    (2) heuristic policy of beating the last opponent move
    (3) LSTM/feedforward PPO policies
    (4) LSTM policy with custom entropy loss
"""

import argparse
import os
from pettingzoo.classic import rps_v2
import random

import ray
from ray import air, tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import (
    PPO,
    PPOConfig,
    PPOTF1Policy,
    PPOTF2Policy,
    PPOTorchPolicy,
)
from ray.rllib.env import PettingZooEnv
from ray.rllib.examples.policy.rock_paper_scissors_dummies import (
    BeatLastHeuristic,
    AlwaysSameHeuristic,
)
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import register_env

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=150, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward",
    type=float,
    default=1000.0,
    help="Reward at which we stop training.",
)


def env_creator(args):
    env = rps_v2.env()
    return env


register_env("RockPaperScissors", lambda config: PettingZooEnv(env_creator(config)))


def run_heuristic_vs_learned(args, use_lstm=False, algorithm_config=None):
    """Run heuristic policies vs a learned agent.

    The learned agent should eventually reach a reward of ~5 with
    use_lstm=False, and ~7 with use_lstm=True. The reason the LSTM policy
    can perform better is since it can distinguish between the always_same vs
    beat_last heuristics.
    """

    def select_policy(agent_id, episode, **kwargs):
        if agent_id == "player_0":
            return "learned"
        else:
            return random.choice(["always_same", "beat_last"])

    config = (
        (algorithm_config or PPOConfig())
        .environment("RockPaperScissors")
        .framework(args.framework)
        .rollouts(
            num_rollout_workers=0,
            num_envs_per_worker=4,
        )
        .multi_agent(
            policies={
                "always_same": PolicySpec(policy_class=AlwaysSameHeuristic),
                "beat_last": PolicySpec(policy_class=BeatLastHeuristic),
                "learned": PolicySpec(
                    config=AlgorithmConfig.overrides(
                        model={"use_lstm": use_lstm},
                        framework_str=args.framework,
                    )
                ),
            },
            policy_mapping_fn=select_policy,
            policies_to_train=["learned"],
        )
        .reporting(metrics_num_episodes_for_smoothing=200)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    algo = config.build()

    reward_diff = 0
    for _ in range(args.stop_iters):
        results = algo.train()
        # Timesteps reached.
        if "policy_always_same_reward" not in results["hist_stats"]:
            reward_diff = 0
            continue
        reward_diff = sum(results["hist_stats"]["policy_learned_reward"])
        print(f"delta_r={reward_diff}")
        if results["timesteps_total"] > args.stop_timesteps:
            break
        # Reward (difference) reached -> all good, return.
        elif reward_diff > args.stop_reward:
            return

    # Reward (difference) not reached: Error if `as_test`.
    if args.as_test:
        raise ValueError(
            "Desired reward difference ({}) not reached! Only got to {}.".format(
                args.stop_reward, reward_diff
            )
        )


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init()

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    run_heuristic_vs_learned(args, use_lstm=False)
    print("run_heuristic_vs_learned(w/o lstm): ok.")

    run_heuristic_vs_learned(args, use_lstm=True)
    print("run_heuristic_vs_learned (w/ lstm): ok.")
