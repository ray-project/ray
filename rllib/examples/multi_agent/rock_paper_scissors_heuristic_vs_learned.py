"""A simple multi-agent env with two agents play rock paper scissors.

This demonstrates running the following policies in competition:
    Agent 1: heuristic policy of repeating the same move
             OR: heuristic policy of beating the last opponent move
    Agent 2: Simple, feedforward PPO policy
             OR: PPO Policy with an LSTM network

How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=2 [--use-lstm]?`

Without `--use-lstm`, Agent 2 should quickly reach a reward of ~7.0, always
beating the `always_same` policy, but only 50% of the time beating the `beat_last`
policy.

With `--use-lstm`, Agent 2 should eventually(!) reach a reward of >9.0 (always
beating both the `always_same` policy and the `beat_last` policy).

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`
"""

import random

import gymnasium as gym
from pettingzoo.classic import rps_v2

from ray.air.constants import TRAINING_ITERATION
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.examples.rl_modules.classes import (
    AlwaysSameHeuristicRLM,
    BeatLastHeuristicRLM,
)
from ray.tune.registry import get_trainable_cls, register_env


parser = add_rllib_example_script_args(
    default_iters=50,
    default_timesteps=200000,
    default_reward=6.0,
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_agents=2,
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

    assert args.num_agents == 2, "Must set --num-agents=2 when running this script!"

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("RockPaperScissors")
        .env_runners(
            env_to_module_connector=lambda env: (
                # `agent_ids=...`: Only flatten obs for the learning RLModule.
                FlattenObservations(multi_agent=True, agent_ids={"player_0"}),
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
        .training(
            vf_loss_coeff=0.005,
        )
        .rl_module(
            model_config_dict={
                "use_lstm": args.use_lstm,
                # Use a simpler FCNet when we also have an LSTM.
                "fcnet_hiddens": [32] if args.use_lstm else [256, 256],
                "lstm_cell_size": 256,
                "max_seq_len": 15,
                "vf_share_layers": True,
            },
            rl_module_spec=MultiRLModuleSpec(
                module_specs={
                    "always_same": RLModuleSpec(
                        module_class=AlwaysSameHeuristicRLM,
                        observation_space=gym.spaces.Discrete(4),
                        action_space=gym.spaces.Discrete(3),
                    ),
                    "beat_last": RLModuleSpec(
                        module_class=BeatLastHeuristicRLM,
                        observation_space=gym.spaces.Discrete(4),
                        action_space=gym.spaces.Discrete(3),
                    ),
                    "learned": RLModuleSpec(),
                }
            ),
        )
    )

    # Make `args.stop_reward` "point" to the reward of the learned policy.
    stop = {
        TRAINING_ITERATION: args.stop_iters,
        f"{ENV_RUNNER_RESULTS}/module_episode_returns_mean/learned": args.stop_reward,
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    }

    run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        success_metric={
            f"{ENV_RUNNER_RESULTS}/module_episode_returns_mean/learned": (
                args.stop_reward
            ),
        },
    )
