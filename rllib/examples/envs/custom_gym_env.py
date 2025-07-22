"""Example of defining a custom gymnasium Env to be learned by an RLlib Algorithm.

This example:
    - demonstrates how to write your own (single-agent) gymnasium Env class, define its
    physics and mechanics, the reward function used, the allowed actions (action space),
    and the type of observations (observation space), etc..
    - shows how to configure and setup this environment class within an RLlib
    Algorithm config.
    - runs the experiment with the configured algo, trying to solve the environment.

To see more details on which env we are building for this example, take a look at the
`SimpleCorridor` class defined below.


How to run this script
----------------------
`python [script file name].py`

Use the `--corridor-length` option to set a custom length for the corridor. Note that
for extremely long corridors, the algorithm should take longer to learn.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see results similar to the following in your console output:

+--------------------------------+------------+-----------------+--------+
| Trial name                     | status     | loc             |   iter |
|--------------------------------+------------+-----------------+--------+
| PPO_SimpleCorridor_78714_00000 | TERMINATED | 127.0.0.1:85794 |      7 |
+--------------------------------+------------+-----------------+--------+

+------------------+-------+----------+--------------------+
|   total time (s) |    ts |   reward |   episode_len_mean |
|------------------+-------+----------+--------------------|
|          18.3034 | 28000 | 0.908918 |            12.9676 |
+------------------+-------+----------+--------------------+
"""
# These tags allow extracting portions of this script on Anyscale.
# ws-template-imports-start
import gymnasium as gym
from gymnasium.spaces import Discrete, Box
import numpy as np
import random

from typing import Optional

# ws-template-imports-end

from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env  # noqa


parser = add_rllib_example_script_args(
    default_reward=0.9,
    default_iters=50,
    default_timesteps=100000,
)
parser.add_argument(
    "--corridor-length",
    type=int,
    default=10,
    help="The length of the corridor in fields. Note that this number includes the "
    "starting- and goal states.",
)


# These tags allow extracting portions of this script on Anyscale.
# ws-template-code-start
class SimpleCorridor(gym.Env):
    """Example of a custom env in which the agent has to walk down a corridor.

    ------------
    |S........G|
    ------------
    , where S is the starting position, G is the goal position, and fields with '.'
    mark free spaces, over which the agent may step. The length of the above example
    corridor is 10.
    Allowed actions are left (0) and right (1).
    The reward function is -0.01 per step taken and a uniform random value between
    0.5 and 1.5 when reaching the goal state.

    You can configure the length of the corridor via the env's config. Thus, in your
    AlgorithmConfig, you can do:
    `config.environment(env_config={"corridor_length": ..})`.
    """

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self.end_pos = config.get("corridor_length", 7)
        self.cur_pos = 0
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, self.end_pos, shape=(1,), dtype=np.float32)

    def reset(self, *, seed=None, options=None):
        random.seed(seed)
        self.cur_pos = 0
        # Return obs and (empty) info dict.
        return np.array([self.cur_pos], np.float32), {"env_state": "reset"}

    def step(self, action):
        assert action in [0, 1], action
        # Move left.
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        # Move right.
        elif action == 1:
            self.cur_pos += 1

        # The environment only ever terminates when we reach the goal state.
        terminated = self.cur_pos >= self.end_pos
        truncated = False
        # Produce a random reward from [0.5, 1.5] when we reach the goal.
        reward = random.uniform(0.5, 1.5) if terminated else -0.01
        infos = {}
        return (
            np.array([self.cur_pos], np.float32),
            reward,
            terminated,
            truncated,
            infos,
        )


# ws-template-code-end

if __name__ == "__main__":
    args = parser.parse_args()

    # Can also register the env creator function explicitly with:
    # register_env("corridor-env", lambda config: SimpleCorridor())

    # Or you can hard code certain settings into the Env's constructor (`config`).
    # register_env(
    #    "corridor-env-w-len-100",
    #    lambda config: SimpleCorridor({**config, **{"corridor_length": 100}}),
    # )

    # Or allow the RLlib user to set more c'tor options via their algo config:
    # config.environment(env_config={[c'tor arg name]: [value]})
    # register_env("corridor-env", lambda config: SimpleCorridor(config))

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            SimpleCorridor,  # or provide the registered string: "corridor-env"
            env_config={"corridor_length": args.corridor_length},
        )
    )

    run_rllib_example_script_experiment(base_config, args)
