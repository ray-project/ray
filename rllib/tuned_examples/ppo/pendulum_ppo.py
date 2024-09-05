from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(default_timesteps=600000, default_reward=-300)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .env_runners(
        num_env_runners=2,
        num_envs_per_env_runner=10,
    )
    .environment("Pendulum-v1")
    .training(
        lr=0.0003,
        lambda_=0.1,
        vf_clip_param=10.0,
        num_epochs=6,
    )
    .rl_module(
        model_config_dict={
            "fcnet_activation": "relu",
            "uses_new_env_runners": True,
        },
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
