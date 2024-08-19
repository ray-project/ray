from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=200000,
)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

config = (
    DQNConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment(env="CartPole-v1")
    .training(
        lr=0.0005 * (args.num_gpus or 1) ** 0.5,
        train_batch_size_per_learner=32,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 50000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        n_step=(2, 5),
        double_q=True,
        num_atoms=1,
        noisy=False,
        dueling=True,
    )
    .rl_module(
        # Settings identical to old stack.
        model_config_dict={
            "fcnet_hiddens": [256],
            "fcnet_activation": "tanh",
            "epsilon": [(0, 1.0), (10000, 0.02)],
            "fcnet_bias_initializer": "zeros_",
            "post_fcnet_bias_initializer": "zeros_",
            "post_fcnet_hiddens": [256],
        },
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
