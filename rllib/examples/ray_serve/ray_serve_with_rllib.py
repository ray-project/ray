"""Example on how to run RLlib in combination with Ray Serve.

This example trains an agent with PPO on the CartPole environment, then creates
an RLModule checkpoint and returns its location. After that, it sends the checkpoint
to the Serve deployment for serving the trained RLModule (policy).

This example:
    - shows how to set up a Ray Serve deployment for serving an already trained
    RLModule (policy network).
    - shows how to request new actions from the Ray Serve deployment while actually
    running through episodes in an environment (on which the RLModule that's served
    was trained).


How to run this script
----------------------
`python [script file name].py --stop-reward=200.0`

Use the `--stop-iters`, `--stop-reward`, and/or `--stop-timesteps` options to
determine how long to train the policy for. Use the `--serve-episodes` option to
set the number of episodes to serve (after training) and the `--no-render` option
to NOT render the environment during the serving phase.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.


Results to expect
-----------------

You should see something similar to the following on the command line when using the
options: `--stop-reward=250.0`, `--num-episodes-served=2`, and `--port=12345`:

[First, the RLModule is trained through PPO]

+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_84778_00000 | TERMINATED | 127.0.0.1:40411 |      1 |
+-----------------------------+------------+-----------------+--------+
+------------------+---------------------+------------------------+
|   total time (s) | episode_return_mean |   num_env_steps_sample |
|                  |                     |             d_lifetime |
|------------------+---------------------|------------------------|
|          2.87052 |               253.2 |                  12000 |
+------------------+---------------------+------------------------+

[The RLModule is deployed through Ray Serve on port 12345]

Started Ray Serve with PID: 40458

[A few episodes are played through using the policy service (w/ greedy, non-exploratory
actions)]

Episode R=500.0
Episode R=500.0
"""

import atexit
import os

import requests
import subprocess
import time

import gymnasium as gym
from pathlib import Path

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core import (
    COMPONENT_LEARNER_GROUP,
    COMPONENT_LEARNER,
    COMPONENT_RL_MODULE,
    DEFAULT_MODULE_ID,
)
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray._common.network_utils import build_address

parser = add_rllib_example_script_args()
parser.set_defaults(
    checkpoint_freq=1,
    checkpoint_at_and=True,
)
parser.add_argument("--num-episodes-served", type=int, default=2)
parser.add_argument("--no-render", action="store_true")
parser.add_argument("--port", type=int, default=12345)


def kill_proc(proc):
    try:
        proc.terminate()  # Send SIGTERM
        proc.wait(timeout=5)  # Wait for process to terminate
    except subprocess.TimeoutExpired:
        proc.kill()  # Send SIGKILL
        proc.wait()  # Ensure process is dead


if __name__ == "__main__":
    args = parser.parse_args()

    # Config for the served RLlib RLModule/Algorithm.
    base_config = PPOConfig().environment("CartPole-v1")

    results = run_rllib_example_script_experiment(base_config, args)
    algo_checkpoint = results.get_best_result(
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    ).checkpoint.path
    # We only need the RLModule component from the algorithm checkpoint. It's located
    # under "[algo checkpoint dir]/learner_group/learner/rl_module/[default policy ID]
    rl_module_checkpoint = (
        Path(algo_checkpoint)
        / COMPONENT_LEARNER_GROUP
        / COMPONENT_LEARNER
        / COMPONENT_RL_MODULE
        / DEFAULT_MODULE_ID
    )

    path_of_this_file = Path(__file__).parent
    os.chdir(path_of_this_file)
    # Start the serve app with the trained checkpoint.
    serve_proc = subprocess.Popen(
        [
            "serve",
            "run",
            "classes.cartpole_deployment:rl_module",
            f"rl_module_checkpoint={rl_module_checkpoint}",
            f"port={args.port}",
            "route_prefix=/rllib-rlmodule",
        ]
    )
    # Register our `kill_proc` function to be called on exit to stop Ray Serve again.
    atexit.register(kill_proc, serve_proc)
    # Wait a while to make sure the app is ready to serve.
    time.sleep(20)
    print(f"Started Ray Serve with PID: {serve_proc.pid}")

    try:
        # Create the environment that we would like to receive
        # served actions for.
        env = gym.make("CartPole-v1", render_mode="human")
        obs, _ = env.reset()

        num_episodes = 0
        episode_return = 0.0

        while num_episodes < args.num_episodes_served:
            # Render env if necessary.
            if not args.no_render:
                env.render()

            # print(f"-> Requesting action for obs={obs} ...", end="")
            # Send a request to serve.
            resp = requests.get(
                f"http://{build_address('localhost', args.port)}/rllib-rlmodule",
                json={"observation": obs.tolist()},
            )
            response = resp.json()
            # print(f" received: action={response['action']}")

            # Apply the action in the env.
            action = response["action"]
            obs, reward, terminated, truncated, _ = env.step(action)
            episode_return += reward

            # If episode done -> reset to get initial observation of new episode.
            if terminated or truncated:
                print(f"Episode R={episode_return}")
                obs, _ = env.reset()
                num_episodes += 1
                episode_return = 0.0

    finally:
        # Make sure to kill the process on script termination
        kill_proc(serve_proc)
