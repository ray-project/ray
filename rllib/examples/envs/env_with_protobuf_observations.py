"""Example of handling an Env that outputs protobuf observations.

This example:
    - demonstrates how a custom Env can use protobufs to compress its observation into
    a binary format to save space and gain performance.
    - shows how to use a very simple ConnectorV2 piece that translates these protobuf
    binary observation strings into proper more NN-readable observations (like a 1D
    float32 tensor).

To see more details on which env we are building for this example, take a look at the
`CartPoleWithProtobufObservationSpace` class imported below.
To see more details on which ConnectorV2 piece we are plugging into the config
below, take a look at the `ProtobufCartPoleObservationDecoder` class imported below.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

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

+------------------------------------------------------+------------+-----------------+
| Trial name                                           | status     | loc             |
|                                                      |            |                 |
|------------------------------------------------------+------------+-----------------+
| PPO_CartPoleWithProtobufObservationSpace_47dd2_00000 | TERMINATED | 127.0.0.1:67325 |
+------------------------------------------------------+------------+-----------------+
+--------+------------------+------------------------+------------------------+
|   iter |   total time (s) |   episode_return_mean  |   num_episodes_lifetim |
|        |                  |                        |                      e |
+--------+------------------+------------------------+------------------------+
|     17 |          39.9011 |                 513.29 |                    465 |
+--------+------------------+------------------------+------------------------+
"""
from ray.rllib.examples.connectors.classes.protobuf_cartpole_observation_decoder import (  # noqa
    ProtobufCartPoleObservationDecoder,
)
from ray.rllib.examples.envs.classes.cartpole_with_protobuf_observation_space import (
    CartPoleWithProtobufObservationSpace,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


parser = add_rllib_example_script_args(default_timesteps=200000, default_reward=400.0)
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo).get_default_config()
        # Set up the env to be CartPole-v1, but with protobuf observations.
        .environment(CartPoleWithProtobufObservationSpace)
        # Plugin our custom ConnectorV2 piece to translate protobuf observations
        # (box of dtype uint8) into NN-readible ones (1D tensor of dtype flaot32).
        .env_runners(
            env_to_module_connector=lambda env: ProtobufCartPoleObservationDecoder(),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
