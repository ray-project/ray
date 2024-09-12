from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(default_reward=450.0, default_timesteps=200000)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()

config = (
    PPOConfig()
    .environment("CartPole-v1")
    .training(
        lr=0.0003,
        num_sgd_iter=6,
        vf_loss_coeff=0.01,
    )
    .rl_module(
        model_config_dict={
            "fcnet_hiddens": [32],
            "fcnet_activation": "linear",
            "vf_share_layers": True,
        }
    )
    .evaluation(
        evaluation_num_env_runners=1,
        evaluation_interval=1,
        evaluation_parallel_to_training=True,
        evaluation_config=PPOConfig.overrides(exploration=False),
    )
)

stop = {
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": args.stop_timesteps,
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": (
        args.stop_reward
    ),
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
