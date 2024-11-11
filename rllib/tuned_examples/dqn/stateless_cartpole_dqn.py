from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_timesteps=2000000,
    default_reward=350.0,
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_env_runners=3,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

config = (
    DQNConfig()
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment(env=StatelessCartPole)
    .training(
        lr=0.0003,
        train_batch_size_per_learner=128,
        replay_buffer_config={
            "type": "EpisodeReplayBuffer",
            "capacity": 150000,
        },
        n_step=1,
        double_q=False,
        dueling=False,
        num_atoms=1,
        epsilon=[(0, 1.0), (100000, 0.02)],
    )
    .rl_module(
        # Settings identical to old stack.
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256],
            fcnet_activation="tanh",
            fcnet_bias_initializer="zeros_",
            head_fcnet_bias_initializer="zeros_",
            head_fcnet_hiddens=[256],
            lstm_kernel_initializer="xavier_uniform_",
            use_lstm=True,
            max_seq_len=20,
        ),
    )
)
args.no_tune = True
# args.local_mode=True
if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
