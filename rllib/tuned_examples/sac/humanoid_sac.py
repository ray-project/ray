"""This is WIP.

On a single-GPU machine, with the `--num-gpus-per-learner=1` command line option, this
example should learn a episode return of >1000 in ~10h, which is still very basic, but
does somewhat prove SAC's capabilities. Some more hyperparameter fine tuning, longer
runs, and more scale (`--num-learners > 0` and `--num-env-runners > 0`) should help push
this up.
"""

from torch import nn

from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_timesteps=1000000,
    default_reward=12000.0,
    default_iters=2000,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    SACConfig()
    .environment("Humanoid-v4")
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
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
