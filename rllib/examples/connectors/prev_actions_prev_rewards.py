"""Example using a ConnectorV2 to add previous rewards/actions to an RLModule's input.

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
    - shows how the `PrevActionsPrevRewards` ConnectorV2 piece can be added to the
    env-to-module pipeline to extract previous rewards and/or actions from the ongoing
    episodes.
    - shows how this connector creates  and wraps this new information (rewards and
    actions) together with the original observations into the RLModule's input dict
    under a new `gym.spaces.Dict` structure (for example, if your observation space
    is `O=Box(shape=(3,))` and you add the most recent 1 reward, the new observation
    space will be `Dict({"_original_obs": O, "prev_n_rewards": Box(shape=())})`.
    - demonstrates how to use RLlib's `FlattenObservations` right after the
    `PrevActionsPrevRewards` to flatten that new dict observation structure again into
    a single 1D tensor.
    - uses the StatelessCartPole environment, a CartPole-v1 derivative that's missing
    both x-veloc and angle-veloc observation components and is therefore non-Markovian
    (only partially observable). An LSTM default model is used for training. Adding
    the additional context to the observations (for example, prev. actions) helps the
    LSTM to more quickly learn in this environment.


How to run this script
----------------------
`python [script file name].py --num-frames=4 --env=ALE/Pong-v5`

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

You should see something similar to this in your terminal output when running
ths script as described above:

+---------------------+------------+-----------------+--------+------------------+
| Trial name          | status     | loc             |   iter |   total time (s) |
|                     |            |                 |        |                  |
|---------------------+------------+-----------------+--------+------------------+
| PPO_env_0edd2_00000 | TERMINATED | 127.0.0.1:12632 |     17 |          42.6898 |
+---------------------+------------+-----------------+--------+------------------+
+------------------------+------------------------+------------------------+
|   num_env_steps_sample |   num_env_steps_traine |   episode_return_mean  |
|             d_lifetime |             d_lifetime |                        |
|------------------------+------------------------+------------------------|
|                  68000 |                  68000 |                 205.22 |
+------------------------+------------------------+------------------------+
"""
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import (
    FlattenObservations,
    PrevActionsPrevRewards,
)
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentStatelessCartPole
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune import register_env

torch, nn = try_import_torch()


parser = add_rllib_example_script_args(
    default_reward=200.0, default_timesteps=1000000, default_iters=2000
)
parser.add_argument("--n-prev-rewards", type=int, default=1)
parser.add_argument("--n-prev-actions", type=int, default=1)


if __name__ == "__main__":
    args = parser.parse_args()

    # Define our custom connector pipelines.
    def _env_to_module(env, spaces, device):
        # Create the env-to-module connector pipeline.
        return [
            PrevActionsPrevRewards(
                multi_agent=args.num_agents > 0,
                n_prev_rewards=args.n_prev_rewards,
                n_prev_actions=args.n_prev_actions,
            ),
            FlattenObservations(multi_agent=args.num_agents > 0),
        ]

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentStatelessCartPole(
                config={"num_agents": args.num_agents}
            ),
        )
    else:
        register_env("env", lambda _: StatelessCartPole())

    config = (
        PPOConfig()
        .environment("env")
        .env_runners(env_to_module_connector=_env_to_module)
        .training(
            num_epochs=6,
            lr=0.0003,
            train_batch_size=4000,
            vf_loss_coeff=0.01,
        )
        .rl_module(
            model_config=DefaultModelConfig(
                use_lstm=True,
                max_seq_len=20,
                fcnet_hiddens=[32],
                fcnet_activation="linear",
                fcnet_kernel_initializer=nn.init.xavier_uniform_,
                fcnet_bias_initializer=nn.init.constant_,
                fcnet_bias_initializer_kwargs={"val": 0.0},
                vf_share_layers=True,
            ),
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config = config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    run_rllib_example_script_experiment(config, args)
