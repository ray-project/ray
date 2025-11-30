"""
Multi-agent RLlib Footsies Example (PPO)

About:
    - Example is based on the Footsies environment (https://github.com/chasemcd/FootsiesGym).
    - Footsies is a two-player fighting game where each player controls a character and tries to hit the opponent while avoiding being hit.
    - Footsies is a zero-sum game, when one player wins (+1 reward) the other loses (-1 reward).

Summary:
    - Main policy is an LSTM-based policy.
    - Training algorithm is PPO.

Training:
    - Training is governed by adding new, more complex opponents to the mix as the main policy reaches a certain win rate threshold against the current opponent.
    - Current opponent is always the newest opponent added to the mix.
    - Training starts with a very simple opponent: "noop" (does nothing), then progresses to "back" (only moves backwards). These are the fixed (very simple) policies that are used to kick off the training.
    - After "random", new opponents are frozen copies of the main policy at different training stages. They will be added to the mix as "lstm_v0", "lstm_v1", etc.
    - In this way - after kick-starting the training with fixed simple opponents - the main policy will play against a version of itself from an earlier training stage.
    - The main policy has to achieve the win rate threshold against the current opponent to add a new opponent to the mix.
    - Training concludes when the target mix size is reached.

Evaluation:
    - Evaluation is performed against the current (newest) opponent.
    - Evaluation runs for a fixed number of episodes at the end of each training iteration.

"""
import functools
from pathlib import Path

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module import MultiRLModuleSpec, RLModuleSpec
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.examples.envs.classes.multi_agent.footsies.fixed_rlmodules import (
    BackFixedRLModule,
    NoopFixedRLModule,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.footsies_env import (
    env_creator,
)
from ray.rllib.examples.envs.classes.multi_agent.footsies.utils import (
    Matchmaker,
    Matchup,
    MetricsLoggerCallback,
    MixManagerCallback,
)
from ray.rllib.examples.rl_modules.classes.lstm_containing_rlm import (
    LSTMContainingRLModule,
)
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
)
from ray.tune.registry import register_env
from ray.tune.result import TRAINING_ITERATION

# setting two default stopping criteria:
#    1. training_iteration (via "stop_iters")
#    2. num_env_steps_sampled_lifetime (via "default_timesteps")
# ...values very high to make sure that the test passes by adding
# all required policies to the mix, not by hitting the iteration limit.
# Our main stopping criterion is "target_mix_size" (see an argument below).
parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=5_000_000,
)

parser.add_argument(
    "--train-start-port",
    type=int,
    default=45001,
    help="First port number for the Footsies training environment server (default: 45001). Each server gets its own port.",
)
parser.add_argument(
    "--eval-start-port",
    type=int,
    default=55001,
    help="First port number for the Footsies evaluation environment server (default: 55001) Each server gets its own port.",
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
    help="Target binary for Footsies environment (default: linux_server). Linux and Mac machines are supported. "
    "'linux_server' and 'mac_headless' choices are the default options for the training. Game will run in the batchmode, without initializing the graphics. "
    "'linux_windowed' and 'mac_windowed' choices are for the local run only, because "
    "game will be rendered in the OS window. To use this option effectively, set up: "
    "--no-tune --num-env-runners 0 --evaluation-num-env-runners 0",
)
parser.add_argument(
    "--win-rate-threshold",
    type=float,
    default=0.8,
    help="The main policy should have at least 'win-rate-threshold' win rate against the "
    "other policy to advance to the next level. Moving to the next level "
    "means adding a new policy to the mix.",
)
parser.add_argument(
    "--target-mix-size",
    type=int,
    default=5,
    help="Target number of policies (RLModules) in the mix to consider the test passed. "
    "The initial mix size is 2: 'main policy' vs. 'other'. "
    "`--target-mix-size=5` means that 3 new policies will be added to the mix. "
    "Whether to add new policy is decided by checking the '--win-rate-threshold' condition. ",
)
parser.add_argument(
    "--rollout-fragment-length",
    type=int,
    default=256,
    help="The length of each rollout fragment to be collected by the EnvRunners when sampling.",
)

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
        num_cpus_per_env_runner=0.5,
        num_envs_per_env_runner=1,
        batch_mode="truncate_episodes",
        rollout_fragment_length=args.rollout_fragment_length,
        episodes_to_numpy=False,
        create_env_on_local_worker=True,
    )
    .training(
        train_batch_size_per_learner=args.rollout_fragment_length
        * (args.num_env_runners or 1),
        lr=1e-4,
        entropy_coeff=0.01,
        num_epochs=10,
        minibatch_size=128,
    )
    .multi_agent(
        policies={
            main_policy,
            "noop",
            "back",
        },
        # this is a starting policy_mapping_fn
        # It will be updated by the MixManagerCallback during training.
        policy_mapping_fn=Matchmaker(
            [Matchup(main_policy, "noop", 1.0)]
        ).agent_to_module_mapping_fn,
        # we only train the main policy, this doesn't change during training.
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
                # for simplicity, all fixed RLModules are added to the config at the start.
                # However, only "noop" is used at the start of training,
                # the others are added to the mix later by the MixManagerCallback.
                "noop": RLModuleSpec(module_class=NoopFixedRLModule),
                "back": RLModuleSpec(module_class=BackFixedRLModule),
            },
        )
    )
    .evaluation(
        evaluation_num_env_runners=args.evaluation_num_env_runners or 1,
        evaluation_sample_timeout_s=120,
        evaluation_interval=1,
        evaluation_duration=10,  # 10 episodes is enough to get a good win rate estimate
        evaluation_duration_unit="episodes",
        evaluation_parallel_to_training=False,
        # we may add new RLModules to the mix at the end of the evaluation stage.
        # Running evaluation in parallel may result in training for one more iteration on the old mix.
        evaluation_force_reset_envs_before_iteration=True,
        evaluation_config={
            "env_config": {"env-for-evaluation": True},
        },  # evaluation_config is used to add an argument to the env creator.
    )
    .callbacks(
        [
            functools.partial(
                MetricsLoggerCallback,
                main_policy=main_policy,
            ),
            functools.partial(
                MixManagerCallback,
                win_rate_threshold=args.win_rate_threshold,
                main_policy=main_policy,
                target_mix_size=args.target_mix_size,
                starting_modules=[main_policy, "noop"],
                fixed_modules_progression_sequence=(
                    "noop",
                    "back",
                ),
            ),
        ]
    )
)

# stopping criteria to be passed to Ray Tune. The main stopping criterion is "mix_size".
# "mix_size" is reported at the end of each training iteration by the MixManagerCallback.
stop = {
    NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    TRAINING_ITERATION: args.stop_iters,
    "mix_size": args.target_mix_size,
}

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    results = run_rllib_example_script_experiment(
        base_config=config,
        args=args,
        stop=stop,
        success_metric={
            "mix_size": args.target_mix_size
        },  # pass the success metric for RLlib's testing framework
    )
