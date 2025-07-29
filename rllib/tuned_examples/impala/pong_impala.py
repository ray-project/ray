import gymnasium as gym

from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.rl_modules.classes.tiny_atari_cnn_rlm import TinyAtariCNN
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import add_rllib_example_script_args
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_reward=20.0,
    default_timesteps=10000000,
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
)
parser.add_argument(
    "--use-tiny-cnn",
    action="store_true",
    help="Whether to use the old API stack's small CNN Atari architecture, stacking "
    "3 CNN layers ([32, 4, 2, same], [64, 4, 2, same], [256, 11, 1, valid]) for the "
    "base features and then a CNN pi-head with an output of [num-actions, 1, 1] and "
    "a Linear(1) layer for the values. The actual RLModule class used can be found "
    "here: ray.rllib.examples.rl_modules.classes.tiny_atari_cnn_rlm",
)
args = parser.parse_args()


def _make_env_to_module_connector(env, spaces, device):
    return FrameStackingEnvToModule(num_frames=4)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        dim=42 if args.use_tiny_cnn else 64,
        framestack=None,
    )


register_env("env", _env_creator)


config = (
    IMPALAConfig()
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
    .env_runners(
        env_to_module_connector=_make_env_to_module_connector,
        num_envs_per_env_runner=5,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size_per_learner=500,
        grad_clip=30.0,
        grad_clip_by="global_norm",
        lr=0.0009 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=1.0,
        entropy_coeff=[[0, 0.02], [3000000, 0.0]],  # <- crucial parameter to finetune
        # Only update connector states and model weights every n training_step calls.
        # broadcast_interval=5,
    )
    .rl_module(
        rl_module_spec=(
            RLModuleSpec(module_class=TinyAtariCNN) if args.use_tiny_cnn else None
        ),
        model_config=(
            {
                "vf_share_layers": True,
                "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
                "conv_activation": "relu",
                "post_fcnet_hiddens": [256],
            }
            if not args.use_tiny_cnn
            else {}
        ),
    )
)

stop = {
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args, stop=stop)
