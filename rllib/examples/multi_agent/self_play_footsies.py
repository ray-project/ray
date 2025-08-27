"""
Multi-agent RLlib Footsies Simplified Example (PPO)

About:
    - This example as a simplified version of "rllib/tuned_examples/ppo/multi_agent_footsies_ppo.py",
      which has more detailed comments and instructions. Please refer to that example for more information.
    - This example is created to test the self-play training progression with footsies.
"""
import functools

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module import RLModuleSpec, MultiRLModuleSpec
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent.footsies.fixed_rlmodules import (
    NoopFixedRLModule,
    BackFixedRLModule,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.utils import (
    Matchup,
    Matchmaker,
    MetricsLoggerCallback,
    MixManagerCallback,
)
from ray.rllib.examples.rl_modules.classes.lstm_containing_rlm import (
    LSTMContainingRLModule,
)
from ray.rllib.tuned_examples.ppo.multi_agent_footsies_ppo import env_creator
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env
from ray.tune.result import TRAINING_ITERATION

parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=5_000_000,
)


if __name__ == "__main__":
    main_policy = "lstm"
    args = parser.parse_args()

    register_env(name="FootsiesEnv", env_creator=env_creator)

    config = (
        PPOConfig()
        .reporting(
            min_time_s_per_iteration=30,
        )
        .environment(
            env="FootsiesEnv",
            env_config={
                "max_t": 1000,
                "frame_skip": 4,
                "observation_delay": 16,
                "train_start_port": 45001,
                "eval_start_port": 55001,
                "host": "localhost",
                "binary_download_dir": "/tmp/ray/binaries/footsies",
                "binary_extract_dir": "/tmp/ray/binaries/footsies",
                "binary_to_download": "linux_server",
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
            train_batch_size_per_learner=256,
            lr=3e-4,
            entropy_coeff=0.01,
            num_epochs=30,
            minibatch_size=128,
        )
        .multi_agent(
            policies={
                main_policy,
                "noop",
                "back",
            },
            policy_mapping_fn=Matchmaker(
                [Matchup(main_policy, "noop", 1.0)]
            ).agent_to_module_mapping_fn,
            policies_to_train=[main_policy],
        )
        .rl_module(
            rl_module_spec=MultiRLModuleSpec(
                rl_module_specs={
                    main_policy: RLModuleSpec(
                        module_class=LSTMContainingRLModule,
                        model_config={
                            "lstm_cell_size": 128,
                            "dense_layers": [128, 128],
                            "max_seq_len": 64,
                        },
                    ),
                    "noop": RLModuleSpec(module_class=NoopFixedRLModule),
                    "back": RLModuleSpec(module_class=BackFixedRLModule),
                },
            )
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            evaluation_duration=10,
            evaluation_duration_unit="episodes",
            evaluation_parallel_to_training=False,
            evaluation_force_reset_envs_before_iteration=True,
            evaluation_config={
                "env_config": {"env-for-evaluation": True},
            },
        )
        .callbacks(
            [
                functools.partial(
                    MetricsLoggerCallback,
                    main_policy=main_policy,
                ),
                functools.partial(
                    MixManagerCallback,
                    win_rate_threshold=0.5,
                    main_policy=main_policy,
                    starting_modules=[main_policy, "noop"],
                    fixed_modules_progression_sequence=("noop", "back"),
                ),
            ]
        )
    )

    stop = {
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        TRAINING_ITERATION: args.stop_iters,
        "mix_size": 6,
    }

    # Run the experiment
    results = run_rllib_example_script_experiment(
        config,
        args,
        stop=stop,
    )
