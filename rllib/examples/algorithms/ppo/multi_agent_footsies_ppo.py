"""Example showing how to train PPO with self-play on a two-player fighting game.

This example demonstrates multi-agent reinforcement learning with self-play using
the Footsies environment (https://github.com/chasemcd/FootsiesGym). Footsies is
a zero-sum two-player fighting game where agents learn to hit opponents while
avoiding being hit. The training uses curriculum learning with progressively
stronger opponents.

This example:
- Trains on the Footsies two-player fighting game environment
- Uses an LSTM-based policy network for temporal reasoning
- Implements self-play with curriculum learning (opponents get harder over time)
- Starts with simple fixed opponents (noop, back) then adds frozen policy copies
- Uses custom callbacks (MetricsLoggerCallback, MixManagerCallback) for opponent
  management
- Expects to reach target mix size of 5 opponents within 5 million timesteps

Training progression:
- Training starts against the "noop" fixed opponent, then adds "back" as part of the curriculum
- When main policy achieves win rate threshold (default 80%) against current
  opponent, a new frozen copy of the main policy is added to the opponent mix
- Training concludes when the target mix size is reached

Evaluation process:
- Evaluation runs after every training iteration (not in parallel with training)
- Each evaluation plays 10 episodes to estimate the main policy's win rate
- The MixManagerCallback checks the win rate after evaluation to decide whether
  to add a new opponent to the mix
- Evaluation must complete before training resumes because new RLModules may be
  added to the mix based on evaluation results

How to run this script
----------------------
`python multi_agent_footsies_ppo.py`

Key command-line options:
- `--win-rate-threshold=0.8`: Win rate needed to advance to next opponent
- `--target-mix-size=5`: Number of opponent policies to add before stopping
- `--render`: Enable game rendering (uses windowed binary instead of headless)
- `--train-start-port=45001`: Starting port for training environment servers
- `--eval-start-port=55001`: Starting port for evaluation environment servers

To run with different configuration:
`python multi_agent_footsies_ppo.py --win-rate-threshold=0.7 --target-mix-size=4`

To scale up with distributed learning using multiple learners and env-runners:
`python multi_agent_footsies_ppo.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python multi_agent_footsies_ppo.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --evaluation-num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
With the default settings, the main policy should progressively defeat each
opponent (noop, back, then frozen copies of itself) and reach a mix size of 5
within 5 million environment timesteps. Success is determined by reaching the
target mix size, indicating the policy learned to beat increasingly skilled
opponents.
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
    platform_for_binary_to_download,
)
from ray.rllib.examples.rl_modules.classes.lstm_containing_rlm import (
    LSTMContainingRLModule,
)
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.metrics import NUM_ENV_STEPS_SAMPLED_LIFETIME
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
parser.add_argument(
    "--log-unity-output",
    action="store_true",
    help="Whether to log Unity output (from the game engine). Default is False.",
    default=False,
)
parser.add_argument(
    "--render",
    action="store_true",
    default=False,
    help="Whether to render the Footsies environment. Default is False.",
)

main_policy = "lstm"
args = parser.parse_args()
register_env(name="FootsiesEnv", env_creator=env_creator)

# Detect platform and choose appropriate binary
binary_to_download = platform_for_binary_to_download(args.render)

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
            "binary_to_download": binary_to_download,
            "log_unity_output": args.log_unity_output,
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
        entropy_coeff=0.025,
        num_epochs=10,
        minibatch_size=256,
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
    results = run_rllib_example_script_experiment(
        base_config=config,
        args=args,
        stop=stop,
        success_metric={"mix_size": args.target_mix_size},
    )
