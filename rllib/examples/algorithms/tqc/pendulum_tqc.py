"""Example showing how to train TQC on the Pendulum-v1 classic control environment.

TQC (Truncated Quantile Critics) is an extension of SAC that uses distributional
critics with quantile regression to reduce overestimation bias. This example
demonstrates TQC on a simple continuous control task suitable for quick experiments.

This example:
- Trains on Pendulum-v1, a classic swing-up control task with continuous actions
- Uses truncated quantile critics with 25 quantiles and 2 critics
- Drops the top 2 quantiles per network to reduce overestimation bias
- Employs prioritized experience replay with 100K capacity
- Scales learning rates based on the number of learners for distributed training
- Uses mixed n-step returns (2 to 5 steps) for improved sample efficiency
- Expects to achieve episode returns of approximately -250 within 20K timesteps

How to run this script
----------------------
`python pendulum_tqc.py`

To run with different configuration:
`python pendulum_tqc.py --num-env-runners=2`

To scale up with distributed learning using multiple learners and env-runners:
`python pendulum_tqc.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python pendulum_tqc.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
With default settings, this example should achieve an episode return of around -250
within 20,000 timesteps. The Pendulum environment has a maximum possible return of 0
(perfect balancing), with typical good performance in the -200 to -300 range.
"""

from torch import nn

from ray.rllib.algorithms.tqc.tqc import TQCConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_timesteps=20000,
    default_reward=-250.0,
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
    .environment("Pendulum-v1")
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
        # Use a smaller learning rate for the policy.
        actor_lr=2e-4 * (args.num_learners or 1) ** 0.5,
        critic_lr=8e-4 * (args.num_learners or 1) ** 0.5,
        alpha_lr=9e-4 * (args.num_learners or 1) ** 0.5,
        lr=None,
        target_entropy="auto",
        n_step=(2, 5),
        tau=0.005,
        train_batch_size_per_learner=256,
        target_network_update_freq=1,
        # TQC-specific parameters
        n_quantiles=25,
        n_critics=2,
        top_quantiles_to_drop_per_net=2,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 1.0,
            "beta": 0.0,
        },
        num_steps_sampled_before_learning_starts=256 * (args.num_learners or 1),
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256, 256],
            fcnet_activation="relu",
            fcnet_kernel_initializer=nn.init.xavier_uniform_,
            head_fcnet_hiddens=[],
            head_fcnet_activation=None,
            head_fcnet_kernel_initializer="orthogonal_",
            head_fcnet_kernel_initializer_kwargs={"gain": 0.01},
        ),
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
