"""Example using a ConnectorV2 for processing observations with a mean/std filter.

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
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

torch, _ = try_import_torch()

parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=500000,
    default_reward=-300.0,
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentPendulum(config={"num_agents": args.num_agents}),
        )

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env" if args.num_agents > 0 else "Pendulum-v1")
        .env_runners(
            # TODO (sven): MAEnvRunner does not support vectorized envs yet
            #  due to gym's env checkers and non-compatability with RLlib's
            #  MultiAgentEnv API.
            num_envs_per_env_runner=1 if args.num_agents > 0 else 20,
            # Define a single connector piece to be prepended to the env-to-module
            # connector pipeline.
            # Alternatively, return a list of n ConnectorV2 pieces (which will then be
            # included in an automatically generated EnvToModulePipeline or return a
            # EnvToModulePipeline directly.
            env_to_module_connector=(
                lambda env: MeanStdFilter(multi_agent=args.num_agents > 0)
            ),
        )
        .training(
            train_batch_size_per_learner=512,
            mini_batch_size_per_learner=64,
            gamma=0.95,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.0003 * (args.num_gpus or 1),
            lambda_=0.1,
            vf_clip_param=10.0,
            vf_loss_coeff=0.01,
        )
        .evaluation(
            evaluation_num_env_runners=1,
            evaluation_parallel_to_training=True,
            evaluation_interval=1,
            evaluation_duration=10,
            evaluation_duration_unit="episodes",
            evaluation_config={
                "explore": False,
                # Do NOT use the eval EnvRunners' ConnectorV2 states. Instead, before
                # each round of evaluation, broadcast the latest training
                # EnvRunnerGroup's ConnectorV2 states (merged from all training remote
                # EnvRunners) to the eval EnvRunnerGroup (and discard the eval
                # EnvRunners' stats).
                "use_worker_filter_stats": False,
            },
        )
    )
    if args.enable_new_api_stack:
        config = config.rl_module(
            model_config_dict={
                "fcnet_activation": "relu",
                "fcnet_weights_initializer": torch.nn.init.xavier_uniform_,
                "fcnet_bias_initializer": torch.nn.init.constant_,
                "fcnet_bias_initializer_config": {"val": 0.0},
                "uses_new_env_runners": True,
            }
        )
    else:
        config = config.training(
            model=dict(
                {
                    "fcnet_activation": "relu",
                    "fcnet_weights_initializer": torch.nn.init.xavier_uniform_,
                    "fcnet_bias_initializer": torch.nn.init.constant_,
                    "fcnet_bias_initializer_config": {"val": 0.0},
                }
            )
        )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config = config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    stop = {
        TRAINING_ITERATION: args.stop_iters,
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": (
            args.stop_reward
        ),
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    }

    run_rllib_example_script_experiment(config, args, stop=stop)
