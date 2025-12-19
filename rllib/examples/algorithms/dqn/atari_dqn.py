"""Example showing how to run Rainbow DQN on Atari environments with frame stacking.

This example demonstrates DQN with Rainbow enhancements (prioritized replay,
n-step returns, distributional RL, noisy nets, double Q-learning, and dueling
networks) on Atari Pong. DQN learns Q-values directly from high-dimensional
sensory inputs using deep neural networks and experience replay.

This example:
- demonstrates how to wrap Atari environments for preprocessing (grayscale,
  64x64 resolution, reward clipping)
- configures a CNN-based model with 4 convolutional layers suitable for
  processing stacked Atari frames
- shows how to use the `FrameStackingEnvToModule` and `FrameStackingLearner`
  ConnectorV2 pieces for proper 4-frame stacking
- uses prioritized experience replay with 1M capacity buffer (alpha=0.5,
  beta=0.4)
- enables Rainbow components: 3-step returns, 51-atom distributional RL,
  noisy networks, double DQN, and dueling architecture
- updates target network every 32,000 steps with hard updates (tau=1.0)

How to run this script
----------------------
`python atari_dqn.py [options]`

To run with default settings on Pong:
`python atari_dqn.py`

To run on a different Atari environment:
`python atari_dqn.py --env=ale_py:ALE/Breakout-v5`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 18.0 on Pong
within 10 million timesteps (40 million frames with 4x frame stacking).
The 80,000 frame warm-up period (num_steps_sampled_before_learning_starts)
fills the replay buffer before training begins.
"""
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
    default_reward=18.0,
    default_timesteps=10_000_000,  # 40 million timesteps
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
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .env_runners(
        # Every 4 agent steps a training update is performed.
        rollout_fragment_length=4,
        num_env_runners=1,
        env_to_module_connector=_make_env_to_module_connector,
        gym_env_vectorize_mode=gym.VectorizeMode.SYNC,
    )
    .training(
        # Note, the paper uses also an Adam epsilon of 0.00015.
        lr=0.0000625,
        n_step=3,
        tau=1.0,
        train_batch_size=32,
        target_network_update_freq=32_000,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 1_000_000,
            "alpha": 0.5,
            # Note the paper used a linear schedule for beta.
            "beta": 0.4,
        },
        # Note, these are frames.
        num_steps_sampled_before_learning_starts=80_000,
        noisy=True,
        num_atoms=51,
        v_min=-10.0,
        v_max=10.0,
        double_q=True,
        dueling=True,
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
