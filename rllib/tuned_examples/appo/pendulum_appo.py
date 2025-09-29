from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=-300.0,
    default_timesteps=100000000,
)
parser.set_defaults(
    num_env_runners=4,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    APPOConfig()
    .environment("Pendulum-v1")
    .env_runners(
        num_envs_per_env_runner=20,
    )
    .learners(num_learners=1)
    .training(
        train_batch_size_per_learner=500,
        circular_buffer_num_batches=16,
        circular_buffer_iterations_per_batch=10,
        target_network_update_freq=2,
        clip_param=0.4,
        lr=0.0003,
        gamma=0.95,
        lambda_=0.5,
        entropy_coeff=0.0,
        use_kl_loss=True,
        kl_coeff=1.0,
        kl_target=0.04,
    )
    .rl_module(
        model_config=DefaultModelConfig(fcnet_activation="relu"),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
