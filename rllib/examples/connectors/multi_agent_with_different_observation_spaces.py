"""Example of a ConnectorV2 mapping global observations to n per-module observations.

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
    - shows how the `DoubleXYPosToDiscreteIndex` and `DoubleXYPosToSingleXYPos`
    ConnectorV2 pieces can be activated for different agents/modules and added to the
    env-to-module pipeline.
    - demonstrates that using various such observation mapping connector pieces allows
    users to map from global, multi-agent observations to individual modules'
    observations.


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
"""
from ray.rllib.examples.envs.classes.multi_agent.double_row_corridor_env import (
    DoubleRowCorridorEnv,
)
from ray.rllib.examples.connectors.classes.multi_agent_with_different_observation_spaces import (  # noqa
    DoubleXYPosToDiscreteIndex,
    DoubleXYPosToSingleXYPos,
)
from ray.rllib.connectors.env_to_module.flatten_observations import (
    FlattenObservations,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

torch, _ = try_import_torch()

parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=500000,
    default_reward=22.0,
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_agents=2,
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(DoubleRowCorridorEnv)
        .env_runners(
            num_envs_per_env_runner=20,
            # Define a list of two connector piece to be prepended to the env-to-module
            # connector pipeline.
            # One for `agent_0` (converting the global observations into
            # position-indices for that agent), the other for `agent_1` (converting
            # the global observations into single x/y coordinates).
            env_to_module_connector=lambda env: [
                DoubleXYPosToDiscreteIndex(agent_id="agent_0"),
                DoubleXYPosToSingleXYPos(agent_id="agent_1"),
                # Only flatten agent_0's observations (b/c these are ints that need to
                # be one-hot'd).
                FlattenObservations(multi_agent=True, agent_ids=["agent_0"]),
            ],
        )
        .training(
            train_batch_size_per_learner=512,
            gamma=0.95,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.0003 * (args.num_learners or 1),
            vf_loss_coeff=0.01,
        )
        .multi_agent(
            policies={"p0", "p1"},
            policy_mapping_fn=lambda aid, eps, **kw: "p0" if aid == "agent_0" else "p1",
        )
    )

    # PPO specific settings.
    if args.algo == "PPO":
        base_config.training(
            minibatch_size=64,
            lambda_=0.1,
            vf_clip_param=10.0,
        )

    run_rllib_example_script_experiment(base_config, args)
