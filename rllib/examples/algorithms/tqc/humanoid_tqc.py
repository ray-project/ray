"""Example showing how to train TQC on the Humanoid-v4 MuJoCo environment.

TQC (Truncated Quantile Critics) is an extension of SAC that uses distributional
critics with quantile regression. By truncating the upper quantiles when computing
target values, TQC reduces overestimation bias that can plague actor-critic methods,
leading to more stable and efficient learning on complex continuous control tasks.

This example:
- Trains on Humanoid-v4, a challenging 17-DoF locomotion task
- Uses truncated quantile critics with 25 quantiles and 2 critics
- Drops the top 2 quantiles per network to reduce overestimation bias
- Employs prioritized experience replay with capacity of 1M transitions
- Uses a large network architecture (1024x1024) suitable for high-dimensional control
- Applies mixed n-step returns (1 to 3 steps) for variance reduction
- Expects to achieve episode returns >12000 with sufficient training

How to run this script
----------------------
`python humanoid_tqc.py --num-env-runners=4`

For faster training, use GPU acceleration and more parallelism:
`python humanoid_tqc.py --num-learners=1 --num-gpus-per-learner=1 --num-env-runners=8`

To scale up with distributed learning using multiple learners and env-runners:
`python humanoid_tqc.py --num-learners=2 --num-env-runners=16`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
On a single-GPU machine with --num-gpus-per-learner=1, this example should learn
an episode return of >1000 within approximately 10 hours. With more hyperparameter
tuning, longer runs, and additional scale, returns of >12000 are achievable.
"""

from torch import nn

from ray.rllib.algorithms.tqc.tqc import TQCConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_timesteps=1_000_000,
    default_reward=12_000.0,
    default_iters=2_000,
)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=8,
    num_learners=1,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    TQCConfig()
    .environment("Humanoid-v4")
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
    )
    .learners(
        num_learners=args.num_learners,
        num_gpus_per_learner=1,
        num_aggregator_actors_per_learner=2,
    )
    .training(
        initial_alpha=1.001,
        actor_lr=0.00005,
        critic_lr=0.00005,
        alpha_lr=0.00005,
        target_entropy="auto",
        n_step=(1, 3),
        tau=0.005,
        train_batch_size_per_learner=256,
        target_network_update_freq=1,
        # TQC-specific parameters
        n_quantiles=25,
        n_critics=2,
        top_quantiles_to_drop_per_net=2,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 1000000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        num_steps_sampled_before_learning_starts=10000,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[1024, 1024],
            fcnet_activation="relu",
            fcnet_kernel_initializer=nn.init.xavier_uniform_,
            head_fcnet_hiddens=[],
            head_fcnet_activation=None,
            head_fcnet_kernel_initializer="orthogonal_",
            head_fcnet_kernel_initializer_kwargs={"gain": 0.01},
        )
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
