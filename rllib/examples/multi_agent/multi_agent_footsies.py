"""Multi-agent RLlib example for Footsies
Example is based on the Footsies environment (https://github.com/chasemcd/FootsiesGym).

Footsies is a two-player fighting game where each player controls a character
and tries to hit the opponent while avoiding being hit.
"""


from pathlib import Path

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module import RLModuleSpec, MultiRLModuleSpec
from ray.rllib.env import EnvContext
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent.footsies.footsies_env import (
    FootsiesEnv,
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
parser.add_argument(
    "--start-port",
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


def env_creator(env_config: EnvContext) -> FootsiesEnv:
    if env_config.get("evaluation", False):
        port = (
            env_config["eval_start_port"]
            - 1  # "-1" to start with eval_start_port as the first port (worker index starts at 1)
            + int(env_config.worker_index) * env_config.get("num_envs_per_worker", 1)
            + env_config.get("vector_index", 0)
        )
    else:
        port = (
            env_config["start_port"]
            + int(env_config.worker_index) * env_config.get("num_envs_per_worker", 1)
            + env_config.get("vector_index", 0)
        )
    return FootsiesEnv(config=env_config, port=port)


if __name__ == "__main__":
    args = parser.parse_args()

    register_env("FootsiesEnv", env_creator)

    config = (
        PPOConfig()
        .reporting(
            min_time_s_per_iteration=45,
        )
        .environment(
            env="FootsiesEnv",
            env_config={
                "max_t": 1000,
                "frame_skip": 4,
                "observation_delay": 16,
                "start_port": args.start_port,
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
            num_env_runners=20,
            num_cpus_per_env_runner=0.2,
            num_envs_per_env_runner=1,
            batch_mode="truncate_episodes",
            rollout_fragment_length=256,
            episodes_to_numpy=False,
        )
        .training(
            train_batch_size_per_learner=256 * 20,
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
                            "dense_layers": [128, 128],
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
            evaluation_duration_unit="timesteps",
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
        # keep_ray_up=True,
    )
