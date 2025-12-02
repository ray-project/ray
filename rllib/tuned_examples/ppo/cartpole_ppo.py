from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(default_reward=450.0, default_timesteps=300000)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .environment("CartPole-v1")
    .training(
        lr=0.0003,
        num_epochs=6,
        vf_loss_coeff=0.01,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[32],
            fcnet_activation="linear",
            vf_share_layers=True,
        ),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
