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
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    TQCConfig()
    .environment("Pendulum-v1")
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
