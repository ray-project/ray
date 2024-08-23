from torch import nn

from ray.rllib.algorithms.sac.sac import SACConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_timesteps=1000000,
    default_reward=12000.0,
    default_iters=2000,
)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    SACConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("HalfCheetah-v4")
    .training(
        initial_alpha=1.001,
        # lr=0.0006 is very high, w/ 4 GPUs -> 0.0012
        # Might want to lower it for better stability, but it does learn well.
        actor_lr=2e-4 * (args.num_gpus or 1) ** 0.5,
        critic_lr=8e-4 * (args.num_gpus or 1) ** 0.5,
        alpha_lr=9e-4 * (args.num_gpus or 1) ** 0.5,
        lr=None,
        target_entropy="auto",
        n_step=(1, 5),  # 1?
        tau=0.005,
        train_batch_size_per_learner=256,
        target_network_update_freq=1,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 100000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        num_steps_sampled_before_learning_starts=10000,
    )
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [256, 256],
            "fcnet_activation": "relu",
            "fcnet_weights_initializer": nn.init.xavier_uniform_,
            "post_fcnet_hiddens": [],
            "post_fcnet_activation": None,
            "post_fcnet_weights_initializer": "orthogonal_",
            "post_fcnet_weights_initializer_config": {"gain": 0.01},
        }
    )
    .reporting(
        metrics_num_episodes_for_smoothing=5,
        min_sample_timesteps_per_iteration=1000,
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
