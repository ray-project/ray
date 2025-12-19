"""Example showing how to run DreamerV3 on Atari environments.

DreamerV3 is a model-based reinforcement learning algorithm that learns a world
model to predict future states and rewards, then uses imagination (dreaming) to
train the policy without additional environment interaction. It achieves
state-of-the-art performance across diverse domains including Atari, continuous
control, and 3D environments.

This example:
    - Runs DreamerV3 on Atari Pong (default) using the ale_py Gymnasium interface
    - Uses the "XL" model size as recommended in the DreamerV3 paper for Atari
    - Configures a training ratio of 64 (gradient steps per environment step)
    - Scales batch size and learning rates with the number of learners
    - Does not use frame stacking (the world model integrates information over time)
    - Expects to reach a reward of 20.0 within 10 million timesteps

How to run this script
----------------------
`python atari_dreamerv3.py [options]`

To run with default settings on Pong:
`python atari_dreamerv3.py`

To run on a different Atari environment:
`python atari_dreamerv3.py --env=ale_py:ALE/Breakout-v5`

To scale up with distributed learning using multiple learners and env-runners:
`python atari_dreamerv3.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python atari_dreamerv3.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 20.0 on Pong
within 10 million timesteps. DreamerV3's world model learns to predict
game dynamics, allowing efficient policy learning through imagination.
Training performance scales with the number of learners - batch size and
learning rates are automatically adjusted when using multiple GPUs.
"""
import gymnasium as gym

from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune import register_env

parser = add_rllib_example_script_args(
    default_reward=18.0,
    default_timesteps=10_000_000,  # 40 million frames
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
)
args = parser.parse_args()


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(args.env, **cfg, **{"render_mode": "rgb_array"}),
        dim=64,
        framestack=None,
    )


register_env("env", _env_creator)


config = (
    DreamerV3Config()
    .environment(
        env="env",
        env_config={
            "repeat_action_probability": 0.0,
            "full_action_space": False,
            "frameskip": 1,
        },
    )
    .env_runners(
        num_envs_per_env_runner=2,
    )
    .learners(
        num_aggregator_actors_per_learner=2,
    )
    .training(
        model_size="XL",
        training_ratio=64,
        batch_size_B=16 * (args.num_learners or 1),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
