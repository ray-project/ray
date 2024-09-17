"""Example using a ConnectorV2 to flatten arbitrarily nested dict or tuple observations.

An RLlib Algorithm has 3 distinct connector pipelines:
- An env-to-module pipeline in an EnvRunner accepting a list of episodes and producing
a batch for an RLModule to compute actions (`forward_inference()` or
`forward_exploration()`).
- A module-to-env pipeline in an EnvRunner taking the RLModule's output and converting
it into an action readable by the environment.
- A learner connector pipeline on a Learner taking a list of episodes and producing
a batch for an RLModule to perform the training forward pass (`forward_train()`).

Each of these pipelines has a fixed set of default ConnectorV2 pieces that RLlib
adds/prepends to these pipelines in order to perform the most basic functionalities.
For example, RLlib adds the `AddObservationsFromEpisodesToBatch` ConnectorV2 into any
env-to-module pipeline to make sure the batch for computing actions contains - at the
minimum - the most recent observation.

On top of these default ConnectorV2 pieces, users can define their own ConnectorV2
pieces (or use the ones available already in RLlib) and add them to one of the 3
different pipelines described above, as required.

This example:
    - shows how the `FlattenObservation` ConnectorV2 piece can be added to the
    env-to-module pipeline.
    - demonstrates that by using this connector, any arbitrarily nested dict or tuple
    observations is properly flattened into a simple 1D tensor, for easier RLModule
    processing.
    - shows how - in a multi-agent setup - individual agents can be specified, whose
    observations should be flattened (while other agents' observations will always
    be left as-is).
    - uses a variant of the CartPole-v1 environment, in which the 4 observation items
    (x-pos, x-veloc, angle, and angle-veloc) are taken apart and put into a nested dict
    with the structure:
    {
        "x-pos": [x-pos],
        "angular-pos": {
            "value": [angle],
            "some_random_stuff": [random Discrete(3)],  # <- should be ignored by algo
        },
        "velocs": Tuple([x-veloc], [angle-veloc]),
    }


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

+---------------------+------------+----------------+--------+------------------+
| Trial name          | status     | loc            |   iter |   total time (s) |
|                     |            |                |        |                  |
|---------------------+------------+----------------+--------+------------------+
| PPO_env_a2fd6_00000 | TERMINATED | 127.0.0.1:7409 |     25 |          24.1426 |
+---------------------+------------+----------------+--------+------------------+
+------------------------+------------------------+------------------------+
|   num_env_steps_sample |   num_env_steps_traine |   episode_return_mean  |
|             d_lifetime |             d_lifetime |                        |
+------------------------+------------------------+------------------------|
|                 100000 |                 100000 |                 421.42 |
+------------------------+------------------------+------------------------+
"""
from ray.tune.registry import register_env
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.examples.envs.classes.cartpole_with_dict_observation_space import (
    CartPoleWithDictObservationSpace,
)
from ray.rllib.examples.envs.classes.multi_agent import (
    MultiAgentCartPoleWithDictObservationSpace,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


# Read in common example script command line arguments.
parser = add_rllib_example_script_args(default_timesteps=200000, default_reward=400.0)


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    # Define env-to-module-connector pipeline for the new stack.
    def _env_to_module_pipeline(env):
        return FlattenObservations(multi_agent=args.num_agents > 0)

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentCartPoleWithDictObservationSpace(
                config={"num_agents": args.num_agents}
            ),
        )
    else:
        register_env("env", lambda _: CartPoleWithDictObservationSpace())

    # Define the AlgorithmConfig used.
    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env")
        .env_runners(env_to_module_connector=_env_to_module_pipeline)
        .training(
            gamma=0.99,
            lr=0.0003,
        )
        .rl_module(
            model_config_dict={
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "vf_share_layers": True,
                "uses_new_env_runners": True,
            },
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # PPO-specific settings (for better learning behavior only).
    if args.algo == "PPO":
        base_config.training(
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
    # IMPALA-specific settings (for better learning behavior only).
    elif args.algo == "IMPALA":
        base_config.training(
            lr=0.0005,
            vf_loss_coeff=0.05,
            entropy_coeff=0.0,
        )
        base_config.rl_module(
            model_config_dict={
                "vf_share_layers": True,
                "uses_new_env_runners": True,
            }
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(base_config, args)
