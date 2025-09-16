from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(default_timesteps=400000, default_reward=-300)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .environment("Pendulum-v1")
    .env_runners(
        num_env_runners=2,
        num_envs_per_env_runner=20,
        env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
    )
    .training(
        train_batch_size_per_learner=1024,
        minibatch_size=128,
        lr=0.0002 * (args.num_learners or 1) ** 0.5,
        gamma=0.95,
        lambda_=0.5,
        # num_epochs=8,
    )
    .rl_module(
        model_config=DefaultModelConfig(fcnet_activation="relu"),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
