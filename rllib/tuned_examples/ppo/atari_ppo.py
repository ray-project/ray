import gymnasium as gym

from ray import tune
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.test_utils import add_rllib_example_script_args


parser = add_rllib_example_script_args(
    default_reward=float("inf"),
    default_timesteps=3000000,
    default_iters=100000000000,
)
parser.set_defaults(enable_new_api_stack=True)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values toset up `config` below.
args = parser.parse_args()


def _make_env_to_module_connector(env):
    return FrameStackingEnvToModule(num_frames=4)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


# Create a custom Atari setup (w/o the usual RLlib-hard-coded framestacking in it).
# We would like our frame stacking connector to do this job.
def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, render_mode="rgb_array"),
        # Perform frame-stacking through ConnectorV2 API.
        framestack=None,
    )


tune.register_env("env", _env_creator)


config = (
    PPOConfig()
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
        # num_envs_per_env_runner=5,  # 5 on old yaml example
        env_to_module_connector=_make_env_to_module_connector,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size_per_learner=4000,  # 5000 on old yaml example
        minibatch_size=128,  # 500 on old yaml example
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.1,
        vf_clip_param=10.0,
        entropy_coeff=0.01,
        num_epochs=10,
        lr=0.00015 * args.num_gpus,
        grad_clip=100.0,
        grad_clip_by="global_norm",
    )
    .rl_module(
        model_config_dict={
            "vf_share_layers": True,
            "conv_filters": [[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            "conv_activation": "relu",
            "post_fcnet_hiddens": [256],
            "uses_new_env_runners": True,
        }
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args=args)
