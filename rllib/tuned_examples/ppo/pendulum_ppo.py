from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(default_timesteps=400000, default_reward=-300)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .environment("Pendulum-v1")
    .env_runners(
        num_env_runners=2,
        num_envs_per_env_runner=20,
        env_to_module_connector=lambda env: MeanStdFilter(),
    )
    .training(
        train_batch_size_per_learner=1024,
        minibatch_size=128,
        lr=0.0002 * (args.num_gpus or 1) ** 0.5,
        gamma=0.95,
        lambda_=0.5,
        # num_epochs=8,
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
