"""Example of running against a TCP-connected external env performing its own inference.

The example uses a custom EnvRunner (TcpClientInferenceEnvRunner) to allow
connections from one or more TCP clients to RLlib's EnvRunner actors, which act as
RL servers.
In this example, action inference for stepping the env is performed on the client's
side, meaning the client computes all actions itself, applies them to the env logic,
collects episodes of experiences, and sends these (in bulk) back to RLlib for training.
Also, from time to time, the updated model weights have to be sent from RLlib (server)
back to the connected clients.
Note that RLlib's new API stack does not yet support individual action requests, where
action computations happen on the RLlib (server) side.

This example:
    - demonstrates how RLlib can be hooked up to an externally running complex simulator
    through TCP connections.
    - shows how a custom EnvRunner subclass can be configured allowing users to
    implement their own logic of connecting to external processes and customize the
    messaging protocols.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --port 5555

Use the `--port` option to change the default port (5555) to some other value.
Make sure that you do the same on the client side.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see something like this on your terminal. Note that the dummy CartPole
client (which runs in a thread for the purpose of this example here) might throw
a disconnection error at the end, b/c RLlib closes the server socket when done training.

+----------------------+------------+--------+------------------+
| Trial name           | status     |   iter |   total time (s) |
|                      |            |        |                  |
|----------------------+------------+--------+------------------+
| PPO_None_3358e_00000 | TERMINATED |     40 |          32.2649 |
+----------------------+------------+--------+------------------+
+------------------------+------------------------+
|  episode_return_mean  |   num_env_steps_sample |
|                       |             d_lifetime |
|-----------------------+------------------------|
|                458.68 |                 160000 |
+-----------------------+------------------------+

From the dummy client (thread), you should see at the end:
```
ConnectionError: Error receiving message from peer on socket ...
```
"""
from functools import partial
import threading

import gymnasium as gym
import numpy as np

from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.tcp_client_inference_env_runner import (
    _dummy_client,
    TcpClientInferenceEnvRunner,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_reward=450.0, default_iters=200, default_timesteps=2000000
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_env_runners=1,
)
parser.add_argument(
    "--port",
    type=int,
    default=5555,
    help="The port for RLlib's EnvRunner to listen to for incoming UE5 connections. "
    "You need to specify the same port inside your UE5 `RLlibClient` plugin.",
)


if __name__ == "__main__":
    args = parser.parse_args()

    # Start the dummy CartPole client in a thread (and do its thing in parallel).
    client_thread = threading.Thread(
        target=partial(
            _dummy_client,
            port=args.port
            + (args.num_env_runners if args.num_env_runners is not None else 1),
        ),
    )
    client_thread.start()

    # Define the RLlib (server) config.
    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            observation_space=gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
            action_space=gym.spaces.Discrete(2),
            # EnvRunners listen on `port` + their worker index.
            env_config={"port": args.port},
        )
        .env_runners(
            # Point RLlib to the custom EnvRunner to be used here.
            env_runner_cls=TcpClientInferenceEnvRunner,
        )
        .training(
            num_epochs=10,
            vf_loss_coeff=0.01,
        )
        .rl_module(model_config=DefaultModelConfig(vf_share_layers=True))
    )

    run_rllib_example_script_experiment(base_config, args)
