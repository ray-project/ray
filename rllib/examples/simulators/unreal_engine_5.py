"""Example of running RLlib with a connected UnrealEngine5 (UE5) instance as Env.

The example uses a custom EnvRunner (ExternalTcpSingleAgentEnvRunner) to allow
connections from within a running UE5 Editor to RLlib.
To set up the UE5 side of things, refer to this tutorial here, where we describe step
by step how to run this example.

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
Make sure that you do the same on the UE5 side, inside the `RLlibClient` plugin.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
TODO: Fill in
"""
import gymnasium as gym
import numpy as np

from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.external_tcp_single_agent_env_runner import (
    ExternalTcpSingleAgentEnvRunner
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_reward=0.99, default_iters=200, default_timesteps=1000000
)
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--port",
    type=int,
    default=5555,
    help="The port for RLlib's EnvRunner to listen to for incoming UE5 connections. "
    "You need to specify the same port inside your UE5 `RLlibClient` plugin.",
)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            observation_space=gym.spaces.Box(-1.0, 1.0, (4,), np.float32),
            action_space=gym.spaces.Discrete(4),
        )
        .env_runners(
            # Point RLlib to the custom EnvRunner to be used here.
            env_runner_cls=ExternalTcpSingleAgentEnvRunner,
        )
        .training(
            num_epochs=10,
            vf_loss_coeff=0.01,
        )
        .rl_module(model_config=DefaultModelConfig(vf_share_layers=True))
    )

    run_rllib_example_script_experiment(base_config, args)
