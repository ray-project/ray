"""Example of implementing and configuring a custom (torch) LSTM containing RLModule.

This example:
    - demonstrates how you can subclass the TorchRLModule base class and set up your
    own LSTM-containing NN architecture by overriding the `setup()` method.
    - shows how to override the 3 forward methods: `_forward_inference()`,
    `_forward_exploration()`, and `forward_train()` to implement your own custom forward
    logic(s), including how to handle STATE in- and outputs to and from these calls.
    - explains when each of these 3 methods is called by RLlib or the users of your
    RLModule.
    - shows how you then configure an RLlib Algorithm such that it uses your custom
    RLModule (instead of a default RLModule).

We implement a simple LSTM layer here, followed by a series of Linear layers.
After the last Linear layer, we add fork of 2 Linear (non-activated) layers, one for the
action logits and one for the value function output.

We test the LSTM containing RLModule on the StatelessCartPole environment, a variant
of CartPole that is non-Markovian (partially observable). Only an RNN-network can learn
a decent policy in this environment due to the lack of any velocity information. By
looking at one observation, one cannot know whether the cart is currently moving left or
right and whether the pole is currently moving up or down).


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
You should see the following output (during the experiment) in your console:

"""
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentStatelessCartPole
from ray.rllib.examples.rl_modules.classes.lstm_containing_rlm import (
    LSTMContainingRLModule,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

parser = add_rllib_example_script_args(
    default_reward=300.0,
    default_timesteps=2000000,
)
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    if args.num_agents == 0:
        register_env("env", lambda cfg: StatelessCartPole())
    else:
        register_env("env", lambda cfg: MultiAgentStatelessCartPole(cfg))

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            env="env",
            env_config={"num_agents": args.num_agents},
        )
        .training(
            train_batch_size_per_learner=1024,
            num_epochs=6,
            lr=0.0009,
            vf_loss_coeff=0.001,
            entropy_coeff=0.0,
        )
        .rl_module(
            # Plug-in our custom RLModule class.
            rl_module_spec=RLModuleSpec(
                module_class=LSTMContainingRLModule,
                # Feel free to specify your own `model_config` settings below.
                # The `model_config` defined here will be available inside your
                # custom RLModule class through the `self.model_config`
                # property.
                model_config={
                    "lstm_cell_size": 256,
                    "dense_layers": [256, 256],
                    "max_seq_len": 20,
                },
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
