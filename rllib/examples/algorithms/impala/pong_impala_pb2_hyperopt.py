import gymnasium as gym

from ray import tune
from ray.rllib.algorithms.impala import IMPALAConfig
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
from ray.tune.schedulers.pb2 import PB2

parser = add_rllib_example_script_args()
parser.set_defaults(env="ale_py:ALE/Pong-v5")
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


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        dim=42 if args.use_tiny_cnn else 64,
        # TODO (sven): Use FrameStacking Connector here for some speedup.
        framestack=4,
    )


register_env("env", _env_creator)

pb2_scheduler = PB2(
    time_attr=NUM_ENV_STEPS_SAMPLED_LIFETIME,
    metric=f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}",
    mode="max",
    perturbation_interval=50000,
    # Copy bottom % with top % weights.
    quantile_fraction=0.25,
    hyperparam_bounds={
        "lr": [0.0001, 0.02],
        "gamma": [0.95, 1.0],
        "entropy_coeff": [0.001, 0.025],
        "vf_loss_coeff": [0.1, 1.0],
        "grad_clip": [10, 200],
        "broadcast_interval": [2, 7],
    },
)

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
        num_envs_per_env_runner=5,
    )
    # .training(
    #    train_batch_size_per_learner=500,
    #    grad_clip=40.0,
    #    grad_clip_by="global_norm",
    #    vf_loss_coeff=0.5,
    #    entropy_coeff=0.008,
    #    # Only update connector states and model weights every n training_step calls.
    #    broadcast_interval=5,
    #    lr=0.009 * ((args.num_learners or 1) ** 0.5),
    # )
    .training(
        train_batch_size_per_learner=tune.randint(256, 1024),
        grad_clip=tune.choice([10, 40, 100, 200]),
        grad_clip_by="global_norm",
        vf_loss_coeff=tune.uniform(0.1, 1.0),
        entropy_coeff=tune.choice([0.001, 0.025]),
        lr=tune.uniform(0.0001, 0.02),
        # Only update connector states and model weights every n training_step calls.
        broadcast_interval=tune.randint(2, 7),
        gamma=tune.uniform(0.95, 1.0),
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
    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 21.0,
    NUM_ENV_STEPS_SAMPLED_LIFETIME: 10000000000,
}


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(
        config, args, stop=stop, scheduler=pb2_scheduler
    )
