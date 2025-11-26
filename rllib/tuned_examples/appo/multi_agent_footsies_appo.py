"""
Multi-agent RLlib Footsies Example (APPO)

About:
    - Example is based on the Footsies environment (https://github.com/chasemcd/FootsiesGym).
    - Footsies is a two-player fighting game where each player controls a character and tries to hit the opponent while avoiding being hit.
    - Footsies is a zero-sum game, when one player wins (+1 reward) the other loses (-1 reward).

Summary:
    - Main policy is an LSTM-based policy.
    - Training algorithm is APPO.

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
import re

from ray import tune
from ray.air.constants import TRAINING_ITERATION
from ray.air.integrations.wandb import WandbLoggerCallback
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module import MultiRLModuleSpec, RLModuleSpec
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
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
)
from ray.tune.registry import register_env

NUM_ENV_RUNNERS = 10

parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=5_000_000,
)

main_policy = "lstm"
args = parser.parse_args()
register_env(name="FootsiesEnv", env_creator=env_creator)

config = (
    APPOConfig()
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
            "binary_to_download": "mac_headless",
            "suppress_unity_output": True,
        },
    )
    .learners(
        num_learners=1,
        num_cpus_per_learner=1,
        num_gpus_per_learner=0,
        num_aggregator_actors_per_learner=0,
    )
    .env_runners(
        num_env_runners=1,
        num_cpus_per_env_runner=1,
        num_envs_per_env_runner=1,
        batch_mode="truncate_episodes",
        rollout_fragment_length=512,
        episodes_to_numpy=True,
        create_env_on_local_worker=False,
    )
    .training(
        train_batch_size_per_learner=4096 * NUM_ENV_RUNNERS,
        lr=1e-4,
        entropy_coeff=0.01,
        num_epochs=10,
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
        evaluation_num_env_runners=1,
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
                win_rate_threshold=0.8,
                main_policy=main_policy,
                target_mix_size=5,
                starting_modules=[main_policy, "noop"],
                fixed_modules_progression_sequence=(
                    "noop",
                    "back",
                ),
            ),
        ]
    )
)


stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    f"{ENV_RUNNER_RESULTS}/{NUM_ENV_STEPS_SAMPLED_LIFETIME}": (args.stop_timesteps),
    TRAINING_ITERATION: args.stop_iters,
    "mix_size": 5,
}

# Log results using WandB.
tune_callbacks = []
if args.wandb_key is not None:
    project = args.wandb_project or (
        args.algo.lower() + "-" + re.sub("\\W+", "-", str(config.env).lower())
    )
    tune_callbacks.append(
        WandbLoggerCallback(
            api_key=args.wandb_key,
            project=project,
            upload_checkpoints=True,
            **({"name": args.wandb_run_name} if args.wandb_run_name else {}),
        )
    )


results = tune.Tuner(
    config.algo_class,
    param_space=config,
    run_config=tune.RunConfig(
        stop=stop,
        verbose=args.verbose,
        callbacks=tune_callbacks,
        checkpoint_config=tune.CheckpointConfig(
            checkpoint_frequency=args.checkpoint_freq,
            checkpoint_at_end=args.checkpoint_at_end,
        ),
    ),
).fit()
