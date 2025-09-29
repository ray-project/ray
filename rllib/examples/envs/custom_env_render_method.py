"""Example of implementing a custom `render()` method for your gymnasium RL environment.

This example:
    - shows how to write a simple gym.Env class yourself, in this case a corridor env,
    in which the agent starts at the left side of the corridor and has to reach the
    goal state all the way at the right.
    - in particular, the new class overrides the Env's `render()` method to show, how
    you can write your own rendering logic.
    - furthermore, we use the RLlib callbacks class introduced in this example here:
    https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_rendering_and_recording.py  # noqa
    in order to compile videos of the worst and best performing episodes in each
    iteration and log these videos to your WandB account, so you can view them.


How to run this script
----------------------
`python [script file name].py
--wandb-key=[your WandB API key] --wandb-project=[some WandB project name]
--wandb-run-name=[optional: WandB run name within --wandb-project]`

In order to see the actual videos, you need to have a WandB account and provide your
API key and a project name on the command line (see above).

Use the `--num-agents` argument to set up the env as a multi-agent env. If
`--num-agents` > 0, RLlib will simply run as many of the defined single-agent
environments in parallel and with different policies to be trained for each agent.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.


Results to expect
-----------------
After the first training iteration, you should see the videos in your WandB account
under the provided `--wandb-project` name. Filter for "videos_best" or "videos_worst".

Note that the default Tune TensorboardX (TBX) logger might complain about the videos
being logged. This is ok, the TBX logger will simply ignore these. The WandB logger,
however, will recognize the video tensors shaped
(1 [batch], T [video len], 3 [rgb], [height], [width]) and properly create a WandB video
object to be sent to their server.

Your terminal output should look similar to this (the following is for a
`--num-agents=2` run; expect similar results for the other `--num-agents`
settings):
+---------------------+------------+----------------+--------+------------------+
| Trial name          | status     | loc            |   iter |   total time (s) |
|---------------------+------------+----------------+--------+------------------+
| PPO_env_fb1c0_00000 | TERMINATED | 127.0.0.1:8592 |      3 |          21.1876 |
+---------------------+------------+----------------+--------+------------------+
+-------+-------------------+-------------+-------------+
|    ts |   combined return |   return p1 |   return p0 |
|-------+-------------------+-------------+-------------|
| 12000 |           12.7655 |      7.3605 |      5.4095 |
+-------+-------------------+-------------+-------------+
"""

import gymnasium as gym
import numpy as np
from gymnasium.spaces import Box, Discrete
from PIL import Image, ImageDraw

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.env.multi_agent_env import make_multi_agent
from ray.rllib.examples.envs.env_rendering_and_recording import EnvRenderCallback
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray import tune

parser = add_rllib_example_script_args(
    default_iters=10,
    default_reward=9.0,
    default_timesteps=10000,
)


class CustomRenderedCorridorEnv(gym.Env):
    """Example of a custom env, for which we specify rendering behavior."""

    def __init__(self, config):
        self.end_pos = config.get("corridor_length", 10)
        self.max_steps = config.get("max_steps", 100)
        self.cur_pos = 0
        self.steps = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 999.0, shape=(1,), dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        self.cur_pos = 0.0
        self.steps = 0
        return np.array([self.cur_pos], np.float32), {}

    def step(self, action):
        self.steps += 1
        assert action in [0, 1], action
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1.0
        elif action == 1:
            self.cur_pos += 1.0
        truncated = self.steps >= self.max_steps
        terminated = self.cur_pos >= self.end_pos
        return (
            np.array([self.cur_pos], np.float32),
            10.0 if terminated else -0.1,
            terminated,
            truncated,
            {},
        )

    def render(self) -> np._typing.NDArray[np.uint8]:
        """Implements rendering logic for this env (given the current observation).

        You should return a numpy RGB image like so:
        np.array([height, width, 3], dtype=np.uint8).

        Returns:
            np.ndarray: A numpy uint8 3D array (image) to render.
        """
        # Image dimensions.
        # Each position in the corridor is 50 pixels wide.
        width = (self.end_pos + 2) * 50
        # Fixed height of the image.
        height = 100

        # Create a new image with white background
        image = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(image)

        # Draw the corridor walls
        # Grey rectangle for the corridor.
        draw.rectangle([50, 30, width - 50, 70], fill="grey")

        # Draw the agent.
        # Calculate the x coordinate of the agent.
        agent_x = (self.cur_pos + 1) * 50
        # Blue rectangle for the agent.
        draw.rectangle([agent_x + 10, 40, agent_x + 40, 60], fill="blue")

        # Draw the goal state.
        # Calculate the x coordinate of the goal.
        goal_x = self.end_pos * 50
        # Green rectangle for the goal state.
        draw.rectangle([goal_x + 10, 40, goal_x + 40, 60], fill="green")

        # Convert the image to a uint8 numpy array.
        return np.array(image, dtype=np.uint8)


# Create a simple multi-agent version of the above Env by duplicating the single-agent
# env n (n=num agents) times and having the agents act independently, each one in a
# different corridor.
MultiAgentCustomRenderedCorridorEnv = make_multi_agent(
    lambda config: CustomRenderedCorridorEnv(config)
)


if __name__ == "__main__":
    args = parser.parse_args()

    # The `config` arg passed into our Env's constructor (see the class' __init__ method
    # above). Feel free to change these.
    env_options = {
        "corridor_length": 10,
        "max_steps": 100,
        "num_agents": args.num_agents,  # <- only used by the multu-agent version.
    }

    env_cls_to_use = (
        CustomRenderedCorridorEnv
        if args.num_agents == 0
        else MultiAgentCustomRenderedCorridorEnv
    )

    tune.register_env("env", lambda _: env_cls_to_use(env_options))

    # Example config switching on rendering.
    base_config = (
        PPOConfig()
        # Configure our env to be the above-registered one.
        .environment("env")
        # Plugin our env-rendering (and logging) callback. This callback class allows
        # you to fully customize your rendering behavior (which workers should render,
        # which episodes, which (vector) env indices, etc..). We refer to this example
        # script here for further details:
        # https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_rendering_and_recording.py  # noqa
        .callbacks(EnvRenderCallback)
    )

    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, eps, **kw: f"p{aid}",
        )

    run_rllib_example_script_experiment(base_config, args)
