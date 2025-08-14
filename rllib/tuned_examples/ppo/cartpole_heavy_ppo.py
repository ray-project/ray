from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.cartpole_with_large_observation_space import (
    CartPoleWithLargeObservationSpace,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(default_reward=450.0, default_timesteps=300000)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .environment(CartPoleWithLargeObservationSpace)
    .env_runners(
        env_to_module_connector=lambda env, spaces, device: FlattenObservations(),
        episodes_to_numpy=False,
    )
    .training(
        lr=0.0003,
        num_epochs=6,
        vf_loss_coeff=0.01,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            use_lstm=True,
            lstm_cell_size=1024,
        ),
    )
)

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
