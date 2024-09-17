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
    - shows how the `MeanStdFilter` ConnectorV2 piece can be added to the env-to-module
    pipeline.
    - demonstrates that using such a filter enhances learning behavior (or even makes
    if possible to learn overall) in some environments, especially those with lopsided
    observation spaces, for example `Box(-3000, -1000, ...)`.


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
Running this example with the mean-std filter results in the normally expected Pendulum
learning behavior:
+-------------------------------+------------+-----------------+--------+
| Trial name                    | status     | loc             |   iter |
|                               |            |                 |        |
|-------------------------------+------------+-----------------+--------+
| PPO_lopsided-pend_f9c96_00000 | TERMINATED | 127.0.0.1:43612 |     77 |
+-------------------------------+------------+-----------------+--------+
+------------------+------------------------+-----------------------+
|   total time (s) |   num_env_steps_sample |   episode_return_mean |
|                  |             d_lifetime |                       |
|------------------+------------------------+-----------------------|
|          30.7466 |                  40040 |                -276.3 |
+------------------+------------------------+-----------------------+

If you try using the `--disable-mean-std-filter` (all other things being equal), you
will either see no learning progress at all (or a very slow one), but more likely some
numerical instability related error will be thrown:

ValueError: Expected parameter loc (Tensor of shape (64, 1)) of distribution
            Normal(loc: torch.Size([64, 1]), scale: torch.Size([64, 1])) to satisfy the
            constraint Real(), but found invalid values:
tensor([[nan],
        [nan],
        [nan],
        ...
"""
import gymnasium as gym
import numpy as np

from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentPendulum
from ray.rllib.utils.framework import try_import_torch
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
parser.add_argument(
    "--disable-mean-std-filter",
    action="store_true",
    help="Run w/o a mean/std env-to-module connector piece (filter).",
)


class LopsidedObs(gym.ObservationWrapper):
    def __init__(self, env):
        super().__init__(env)
        self.observation_space = gym.spaces.Box(-4000.0, -1456.0, (3,), np.float32)

    def observation(self, observation):
        # Lopside [-1.0, 1.0] Pendulum observations
        return ((observation + 1.0) / 2.0) * (4000.0 - 1456.0) - 4000.0


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "lopsided-pend",
            lambda _: MultiAgentPendulum(config={"num_agents": args.num_agents}),
        )
    else:
        register_env("lopsided-pend", lambda _: LopsidedObs(gym.make("Pendulum-v1")))

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("lopsided-pend")
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
                None
                if args.disable_mean_std_filter
                else lambda env: MeanStdFilter(multi_agent=args.num_agents > 0)
            ),
        )
        .training(
            train_batch_size_per_learner=512,
            gamma=0.95,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.0003 * (args.num_gpus or 1),
            vf_loss_coeff=0.01,
        )
        .rl_module(
            model_config_dict={
                "fcnet_activation": "relu",
                "fcnet_weights_initializer": torch.nn.init.xavier_uniform_,
                "fcnet_bias_initializer": torch.nn.init.constant_,
                "fcnet_bias_initializer_config": {"val": 0.0},
                "uses_new_env_runners": True,
            }
        )
        # In case you would like to run with a evaluation EnvRunners, make sure your
        # `evaluation_config` key contains the `use_worker_filter_stats=False` setting
        # (see below). This setting makes sure that the mean/std stats collected by the
        # evaluation EnvRunners are NOT used for the training EnvRunners (unless you
        # really want to mix these stats). It's normally a good idea to keep the stats
        # collected during evaluation completely out of the training data (already for
        # better reproducibility alone).
        # .evaluation(
        #    evaluation_num_env_runners=1,
        #    evaluation_interval=1,
        #    evaluation_config={
        #        "explore": False,
        #        # Do NOT use the eval EnvRunners' ConnectorV2 states. Instead, before
        #        # each round of evaluation, broadcast the latest training
        #        # EnvRunnerGroup's ConnectorV2 states (merged from all training remote
        #        # EnvRunners) to the eval EnvRunnerGroup (and discard the eval
        #        # EnvRunners' stats).
        #        "use_worker_filter_stats": False,
        #    },
        # )
    )

    # PPO specific settings.
    if args.algo == "PPO":
        base_config.training(
            minibatch_size=64,
            lambda_=0.1,
            vf_clip_param=10.0,
        )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    run_rllib_example_script_experiment(base_config, args)
