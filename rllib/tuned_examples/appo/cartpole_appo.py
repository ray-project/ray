from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2000000,
)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()


config = (
    APPOConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment("CartPole-v1")
    .training(
        train_batch_size_per_learner=1000,
        vf_loss_coeff=0.05,
        entropy_coeff=0.01,
        num_epochs=2,
        lr=0.00075,
        minibatch_size=250,
    )
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [32],
            #"vf_share_layers": True,
            "uses_new_env_runners": True,
        },
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
