import gymnasium as gym

from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args()
parser.set_defaults(env="ALE/Pong-v5")
args = parser.parse_args()


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        framestack=4,
    )


register_env("env", _env_creator)


config = (
    ImpalaConfig()
    # Enable new API stack and use EnvRunner.
    .api_stack(
        enable_rl_module_and_learner=True,
        enable_env_runner_and_connector_v2=True,
    )
    .environment(
        "env",
        env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .env_runners(num_envs_per_env_runner=5)
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        vf_loss_coeff=0.1,
        # Only update connector states and model weights every n training_step calls.
        broadcast_interval=5,
    )
    .rl_module(
        model_config_dict={
            "vf_share_layers": True,
            "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            "conv_activation": "relu",
            "post_fcnet_hiddens": [256],
            "uses_new_env_runners": True,
        },
    )
)

stop = {
    "env_runner_results/episode_return_mean": 19.0,
    "num_env_steps_sampled_lifetime": 5000000,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
