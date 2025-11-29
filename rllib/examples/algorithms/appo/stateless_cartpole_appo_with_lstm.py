"""Example showing how to use APPO with an LSTM model on a partially observable environment.

This example demonstrates using APPO (Asynchronous Proximal Policy Optimization)
with a recurrent LSTM model on StatelessCartPole, a modified version of CartPole
where velocity information is removed from the observations. This creates a
partially observable Markov decision process (POMDP) where the agent must
maintain memory of past observations to infer the current velocity.

The standard CartPole observation contains [cart_position, cart_velocity,
pole_angle, pole_angular_velocity]. StatelessCartPole only provides
[cart_position, pole_angle], making it impossible to determine the dynamics
from a single observation alone. The LSTM's hidden state acts as memory,
allowing it to track velocity by observing how positions change over time.

This example:
    - uses `StatelessCartPole` environment to create a partially observable setting
    - configures an LSTM model with `use_lstm=True`, `max_seq_len=20`, and shared
    layers between policy and value function
    - applies `MeanStdFilter` for observation normalization (note: there's a known
    issue that may cause NaNs during training, marked with TODO)
    - demonstrates how recurrent models enable learning in POMDPs where standard
    feedforward networks would fail

How to run this script
----------------------
`python [script file name].py [options]`

To run with default settings (3 env runners):
`python [script file name].py`

To adjust the number of env runners:
`python [script file name].py --num-env-runners=4`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 300.0 within
approximately 2 million timesteps. The LSTM successfully learns to maintain
an internal state that tracks velocity by observing sequential position changes.
Training takes significantly longer than the fully observable CartPole version
(which can solve in ~1M timesteps) due to the additional complexity of learning
with partial observability and the need for the LSTM to develop useful hidden
state representations. The learning curve may show more variance as the LSTM
figures out how to use its memory effectively.
"""
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=300.0,
    default_timesteps=2_000_000,
)
parser.set_defaults(
    num_env_runners=3,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    APPOConfig()
    .environment(StatelessCartPole)
    # TODO (sven): Need to fix the MeanStdFilter(). It seems to cause NaNs when
    #  training.
    # .env_runners(
    #     env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
    # )
    .training(
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        num_epochs=1,
        vf_loss_coeff=0.05,
        entropy_coeff=0.005,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            use_lstm=True,
            max_seq_len=20,
        ),
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
