"""A simple multi-agent env with two agents play rock paper scissors.

This demonstrates running the following policies in competition:
    Agent 1: heuristic policy of repeating the same move
             OR: heuristic policy of beating the last opponent move
    Agent 2: Simple, feedforward PPO policy
             OR: PPO Policy with an LSTM network

When run, Agent 2 should eventually reach a reward of ~7 with use_lstm=False,
and ~?? with use_lstm=True. The reason the LSTM policy performs better is its
ability to distinguish between the `always_same` vs `beat_last` heuristic policies.
"""
import random

import gymnasium as gym
from pettingzoo.classic import rps_v2

from ray.rllib.connectors.env_to_module import (
    AddObservationsFromEpisodesToBatch,
    FlattenObservations,
    WriteObservationsToEpisodes,
)
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.examples.rl_module.classes import (
    AlwaysSameHeuristicRLM,
    BeatLastHeuristicRLM,
)
from ray.tune.registry import get_trainable_cls, register_env


parser = add_rllib_example_script_args(
    default_iters=50,
    default_timesteps=200000,
    default_reward=6.0,
)
parser.add_argument(
    "--use-lstm",
    action="store_true",
    help="Whether to use an LSTM wrapped module instead of a simple MLP one. With LSTM "
    "the reward diff can reach 7.0, without only 5.0.",
)


register_env(
    "RockPaperScissors",
    lambda _: ParallelPettingZooEnv(rps_v2.parallel_env()),
)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("RockPaperScissors")
        .rollouts(
            env_to_module_connector=lambda env: (
                AddObservationsFromEpisodesToBatch(),
                # Only flatten obs for the learning RLModul
                FlattenObservations(multi_agent=True, agent_ids={"player_0"}),
                WriteObservationsToEpisodes(),
            ),
        )
        .multi_agent(
            policies={"always_same", "beat_last", "learned"},
            # Let learning Policy always play against either heuristic one:
            # `always_same` or `beat_last`.
            policy_mapping_fn=lambda aid, episode: (
                "learned"
                if aid == "player_0"
                else random.choice(["always_same", "beat_last"])
            ),
            # Must define this as both heuristic RLMs will throw an error, if their
            # `forward_train` is called.
            policies_to_train=["learned"],
        )
        .training(model={
            "use_lstm": args.use_lstm,
            "lstm_cell_size": 64,
            "max_seq_len": 15,
            "vf_share_layers": True,
            "vf_loss_coeff": 0.01,
        })
        .rl_module(
            rl_module_spec=MultiAgentRLModuleSpec(
                module_specs={
                    "always_same": SingleAgentRLModuleSpec(
                        module_class=AlwaysSameHeuristicRLM,
                        observation_space=gym.spaces.Discrete(4),
                        action_space=gym.spaces.Discrete(3),
                    ),
                    "beat_last": SingleAgentRLModuleSpec(
                        module_class=BeatLastHeuristicRLM,
                        observation_space=gym.spaces.Discrete(4),
                        action_space=gym.spaces.Discrete(3),
                    ),
                    "learned": SingleAgentRLModuleSpec(),
                }
            )
        )
        .evaluation(
            evaluation_interval=1,
            evaluation_num_workers=1,
            evaluation_parallel_to_training=True,
            evaluation_duration="auto",
            evaluation_config={
                "explore": False,
            },
        )
    )

    # Make `args.stop_reward` "point" to the reward of the learned policy.
    stop = {
        "training_iteration": args.stop_iters,
        "evaluation/sampler_results/policy_reward_mean/learned": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        success_metric="evaluation/sampler_results/policy_reward_mean/learned",
    )
