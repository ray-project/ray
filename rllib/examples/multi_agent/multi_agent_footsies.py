"""Multi-agent RLlib example for Footsies
Example is based on the Footsies environment (https://github.com/chasemcd/FootsiesGym).

Footsies is a two-player fighting game where each player controls a character
and tries to hit the opponent while avoiding being hit.
"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module import RLModuleSpec, MultiRLModuleSpec
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent.footsies.footsies_env import (
    FootsiesEnv,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.game.footsies_binary import (
    Config,
    FootsiesBinary,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.utils import (
    Matchup,
    Matchmaker,
)
from ray.rllib.examples.rl_modules.classes.lstm_containing_rlm import (
    LSTMContainingRLModule,
)
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env
from ray.tune.result import TRAINING_ITERATION

eval_policies = []

parser = add_rllib_example_script_args()

if __name__ == "__main__":
    args = parser.parse_args()

    port = 50051

    c = Config(target_binary="mac_headless")
    fb = FootsiesBinary(config=c)
    fb.start_game_server(port=port)

    # Register the environment
    register_env("FootsiesEnv", FootsiesEnv)

    config = (
        PPOConfig()
        .environment(
            env="FootsiesEnv",
            env_config={
                "max_t": 100,
                "frame_skip": 4,
                "observation_delay": 0,
                "port": port,
                "host": "localhost",
            },
        )
        .learners(
            num_learners=1,
            num_cpus_per_learner=1,
            num_gpus_per_learner=0,
            num_aggregator_actors_per_learner=0,
        )
        .env_runners(
            env_runner_cls=MultiAgentEnvRunner,
            num_env_runners=1,
            num_cpus_per_env_runner=1,
            num_envs_per_env_runner=1,
            batch_mode="truncate_episodes",
            rollout_fragment_length=256,
            episodes_to_numpy=False,
        )
        .training(
            # model={"uses_new_env_runners": True},
            lr=3e-4,
            entropy_coeff=0.01,
        )
        .multi_agent(
            policies={
                "lstm",
                "random",
            },
            policy_mapping_fn=Matchmaker(
                [Matchup("lstm", "lstm", 1.0)]
            ).policy_mapping_fn,
            policies_to_train=["lstm"],
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs={
                    "lstm": RLModuleSpec(
                        module_class=LSTMContainingRLModule,
                        model_config={
                            "lstm_cell_size": 32,
                            "dense_layers": [64, 64],
                            "max_seq_len": 32,
                        },
                    ),
                    "random": RLModuleSpec(module_class=RandomRLModule),
                },
            )
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            evaluation_duration="auto",
            evaluation_parallel_to_training=True,
            evaluation_config={
                "env_config": {"evaluation": True},
                "multiagent": {
                    "policy_mapping_fn": Matchmaker(
                        [
                            Matchup(
                                "lstm",
                                eval_policy,
                                1 / (len(eval_policies) + 1),
                            )
                            for eval_policy in eval_policies + ["random"]
                        ]
                    ).policy_mapping_fn,
                },
            },
        )
    )

    stop = {
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        TRAINING_ITERATION: args.stop_iters,
    }

    # Run the experiment
    results = run_rllib_example_script_experiment(
        config,
        args,
        stop=stop,
        keep_ray_up=True,
    )
