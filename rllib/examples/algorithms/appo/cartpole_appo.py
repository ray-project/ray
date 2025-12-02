from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2000000,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    APPOConfig()
    .environment("CartPole-v1")
    .training(
        circular_buffer_iterations_per_batch=2,
        vf_loss_coeff=0.05,
        entropy_coeff=0.0,
    )
    .rl_module(
        model_config=DefaultModelConfig(vf_share_layers=True),
    )
)

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
