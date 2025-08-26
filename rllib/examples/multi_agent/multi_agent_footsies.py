"""Multi-agent RLlib example for Footsies
Example is based on the Footsies environment (https://github.com/chasemcd/FootsiesGym).

Footsies is a two-player fighting game where each player controls a character
and tries to hit the opponent while avoiding being hit.
"""
import functools
from pathlib import Path

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module import RLModuleSpec, MultiRLModuleSpec
from ray.rllib.env import EnvContext
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent.footsies.fixed_rlmodules import (
    NoopFixedRLModule,
    BackFixedRLModule,
    AttackFixedRLModule,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.footsies_env import (
    FootsiesEnv,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.utils import (
    Matchup,
    Matchmaker,
    WinratesCallback,
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

parser = add_rllib_example_script_args()
parser.add_argument(
    "--train-start-port",
    type=int,
    default=45001,
    help="First port number for environment server (default: 45001)",
)
parser.add_argument(
    "--eval-start-port",
    type=int,
    default=55001,
    help="First port number for evaluation environment server (default: 55001)",
)
parser.add_argument(
    "--binary-download-dir",
    type=Path,
    default="/tmp/ray/binaries/footsies",
    help="Directory to download Footsies binaries (default: /tmp/ray/binaries/footsies)",
)
parser.add_argument(
    "--binary-extract-dir",
    type=Path,
    default="/tmp/ray/binaries/footsies",
    help="Directory to extract Footsies binaries (default: /tmp/ray/binaries/footsies)",
)
parser.add_argument(
    "--binary-to-download",
    type=str,
    choices=["linux_server", "linux_windowed", "mac_headless", "mac_windowed"],
    default="linux_server",
    help="Target binary for Footsies environment (default: linux_server)",
)
parser.add_argument(
    "--win-rate-threshold",
    type=float,
    default=0.9,
    help="The main policy should have at least 'win-rate-threshold' win rate against "
    "other policy to advance to the next level. Moving to the next level "
    "means adding a new policy to the mix. The initial mix size is 2: 'main policy' vs 'other'.",
)
parser.add_argument(
    "--target-mix-size",
    type=int,
    default=9,
    help="Target number of policies (RLModules) in the mix to consider the test passed. "
    "The initial mix size is 2: 'main policy' vs. 'other'. "
    "`--target-mix-size=9` means that 7 new policies will be added to the mix. "
    "Whether to add new policy is decided by checking the '--win-rate-threshold' ",
)
parser.add_argument(
    "--rollout-fragment-length",
    type=int,
    default=256,
    help="The length of each rollout fragment to be collected by the EnvRunners.",
)


def env_creator(env_config: EnvContext) -> FootsiesEnv:
    if env_config.get("env-for-evaluation", False):
        port = (
            env_config["eval_start_port"]
            - 1  # "-1" to start with eval_start_port as the first port (worker index starts at 1)
            + int(env_config.worker_index) * env_config.get("num_envs_per_worker", 1)
            + env_config.get("vector_index", 0)
        )
    else:
        port = (
            env_config["train_start_port"]
            + int(env_config.worker_index) * env_config.get("num_envs_per_worker", 1)
            + env_config.get("vector_index", 0)
        )
    return FootsiesEnv(config=env_config, port=port)


if __name__ == "__main__":
    main_policy = "lstm"
    args = parser.parse_args()

    register_env(name="FootsiesEnv", env_creator=env_creator)

    config = (
        PPOConfig()
        .reporting(
            min_time_s_per_iteration=60,
        )
        .environment(
            env="FootsiesEnv",
            env_config={
                "max_t": 1000,
                "frame_skip": 4,
                "observation_delay": 16,
                "train_start_port": args.train_start_port,
                "eval_start_port": args.eval_start_port,
                "host": "localhost",
                "binary_download_dir": args.binary_download_dir,
                "binary_extract_dir": args.binary_extract_dir,
                "binary_to_download": args.binary_to_download,
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
            num_env_runners=args.num_env_runners or 1,
            num_cpus_per_env_runner=1,
            num_envs_per_env_runner=1,
            batch_mode="truncate_episodes",
            rollout_fragment_length=args.rollout_fragment_length,
            episodes_to_numpy=False,
        )
        .training(
            train_batch_size_per_learner=args.rollout_fragment_length
            * (args.num_env_runners or 1),
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
                "attack",
                "random",
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
                    "attack": RLModuleSpec(module_class=AttackFixedRLModule),
                    "random": RLModuleSpec(module_class=RandomRLModule),
                },
            )
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_interval=1,
            # Evaluation duration in episodes is greater than 100 because some episodes may end with both players dead or alive.
            # In this case, the winrates are not logged, and we need to ensure that we have enough episodes to compute the winrates.
            evaluation_duration=100,
            evaluation_duration_unit="episodes",
            evaluation_parallel_to_training=False,  # True,
            evaluation_force_reset_envs_before_iteration=True,
            evaluation_config={
                "env_config": {"env-for-evaluation": True},
            },
        )
        .callbacks(
            functools.partial(
                WinratesCallback,
                win_rate_threshold=args.win_rate_threshold,
                target_mix_size=args.target_mix_size,
                main_policy=main_policy,
                starting_modules=[main_policy, "noop"],
            )
        )
    )

    stop = {
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        TRAINING_ITERATION: args.stop_iters,
        "target_mix_size": args.target_mix_size,
    }

    # Run the experiment
    results = run_rllib_example_script_experiment(
        config,
        args,
        stop=stop,
    )
