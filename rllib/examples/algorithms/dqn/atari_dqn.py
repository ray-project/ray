import gymnasium as gym

from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env

parser = add_rllib_example_script_args(
    default_reward=20.0,
    default_timesteps=10_000_000,
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
)
args = parser.parse_args()


def _make_env_to_module_connector(env, spaces, device):
    return FrameStackingEnvToModule(num_frames=4)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        dim=64,
        framestack=None,
    )


register_env("atari-env", _env_creator)


config = (
    DQNConfig()
    .environment(
        env="atari-env",
        env_config={
            "max_episode_steps": 108000,
            "obs_type": "grayscale",
            # The authors actually use an action repetition of 4.
            "repeat_action_probability": 0.25,
        },
        clip_rewards=True,
    )
    .env_runners(
        # Every 4 agent steps a training update is performed.
        rollout_fragment_length=4,
        num_env_runners=1,
        env_to_module_connector=_make_env_to_module_connector,
    )
    # TODO (simon): Adjust to new model_config_dict.
    .training(
        # Note, the paper uses also an Adam epsilon of 0.00015.
        lr=0.0000625,
        n_step=3,
        tau=1.0,
        train_batch_size=32,
        target_network_update_freq=32000,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 1000000,
            "alpha": 0.5,
            # Note the paper used a linear schedule for beta.
            "beta": 0.4,
        },
        # Note, these are frames.
        num_steps_sampled_before_learning_starts=80000,
        noisy=True,
        num_atoms=51,
        v_min=-10.0,
        v_max=10.0,
        double_q=True,
        dueling=True,
        model={
            "cnn_filter_specifiers": [[32, 8, 4], [64, 4, 2], [64, 3, 1]],
            "fcnet_activation": "tanh",
            "post_fcnet_hiddens": [512],
            "post_fcnet_activation": "relu",
            "post_fcnet_weights_initializer": "orthogonal_",
            "post_fcnet_weights_initializer_config": {"gain": 0.01},
        },
        learner_connector=_make_learner_connector,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            conv_filters=[(16, 4, 2), (32, 4, 2), (64, 4, 2), (128, 4, 2)],
            conv_activation="relu",
            head_fcnet_hiddens=[256],
        )
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
