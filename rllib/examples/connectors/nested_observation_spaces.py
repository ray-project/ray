"""Example using connectors (V2) for observation frame-stacking in Atari environments.

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
    - shows how the `FrameStackingEnvToModule` ConnectorV2 piece can be added to the
    env-to-module pipeline.
    - shows how the `FrameStackingLearner` ConnectorV2 piece can be added to the
    learner connector pipeline.
    - demonstrates that using these two pieces (rather than performing framestacking
    already inside the environment using a gymnasium wrapper) increases overall
    performance by about 5%.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-frames=4 --env=ALE/Pong-v5`

Use the `--num-frames` option to define the number of observations to framestack.
If you don't want to use Connectors to perform the framestacking, set the
`--use-gym-wrapper-framestacking` flag to perform framestacking already inside a
gymnasium observation wrapper. In this case though, be aware that the tensors being
sent through the network are `--num-frames` x larger than if you use the Connector
setup.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------

With `--num-frames=4` and using the two extra ConnectorV2 pieces (in the env-to-module
and learner connector pipelines), you should see something like:
+---------------------------+------------+--------+------------------+...
| Trial name                | status     |   iter |   total time (s) |
|                           |            |        |                  |
|---------------------------+------------+--------+------------------+...
| PPO_atari-env_2fc4a_00000 | TERMINATED |     10 |          557.257 |
+---------------------------+------------+--------+------------------+...

Note that the time to run these 10 iterations is about .% faster than when
performing framestacking already inside the environment (using a
`gymnasium.wrappers.ObservationWrapper`), due to the additional network traffic
needed (sending back 4x[obs] batches instead of 1x[obs] to the learners).

Thus, with the `--use-gym-wrapper-framestacking` option, the output looks
like this:
+---------------------------+------------+--------+------------------+...
| Trial name                | status     |   iter |   total time (s) |
|                           |            |        |                  |
|---------------------------+------------+--------+------------------+...
| PPO_atari-env_2fc4a_00000 | TERMINATED |     10 |          557.257 |
+---------------------------+------------+--------+------------------+...
"""
from ray.tune.registry import register_env
from ray.rllib.connectors.env_to_module import (
    AddObservationsFromEpisodesToBatch,
    FlattenObservations,
    WriteObservationsToEpisodes,
)
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
        return [
            AddObservationsFromEpisodesToBatch(),
            FlattenObservations(multi_agent=args.num_agents > 0),
            WriteObservationsToEpisodes(),
        ]

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
    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env")
        .env_runners(env_to_module_connector=_env_to_module_pipeline)
        .training(
            gamma=0.99,
            lr=0.0003,
        )
    )
    if args.enable_new_api_stack:
        config = config.rl_module(
            model_config_dict={
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "vf_share_layers": True,
                "uses_new_env_runners": True,
            },
        )
    else:
        config = config.training(
            model=dict(
                fcnet_hiddens=[32], fcnet_activation="linear", vf_share_layers=True
            )
        )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config = config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Fix some PPO-specific settings.
    if args.algo == "PPO":
        config = config.training(
            num_sgd_iter=6,
            vf_loss_coeff=0.01,
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(config, args)
