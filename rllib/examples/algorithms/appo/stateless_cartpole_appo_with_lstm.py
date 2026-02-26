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
allowing it to track velocity by observing how the poles position changes over time.

This example:
    - uses `StatelessCartPole` environment to create a partially observable setting
    - configures an LSTM model with `use_lstm=True`, `max_seq_len=20`, and shared
    layers between policy and value function
    - applies `MeanStdFilter` for observation normalization
    - demonstrates how recurrent models enable learning in POMDPs where standard
    feedforward networks would fail

How to run this script
----------------------
`python stateless_cartpole_appo_with_lstm.py [options]`

To run with default settings (4 env runners):
`python stateless_cartpole_appo_with_lstm.py`

To scale up with distributed learning using multiple learners and env-runners:
`python stateless_cartpole_appo_with_lstm.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python stateless_cartpole_appo_with_lstm.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.
By setting `--num-learners=0` and `--num-env-runners=0` will make them run locally
instead of remote Ray Actor where breakpoints aren't possible.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 350.0 (500.0 is the
maximum) within approximately 5 million timesteps (see: `default_timesteps`
in the code). The number of environment
steps can be changed through argparser's `default_timesteps`.
The LSTM should successfully learn to maintain an internal state that
tracks velocity by observing sequential position changes.
This may result in training taking significantly longer than the fully
observable CartPole version due to the additional complexity of learning
with partial observability and the need for the LSTM to develop useful hidden
state representations. Additionally, the learning curve may show more variance
as the LSTM figures out how to use its memory effectively.
"""
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_iters=200,
    default_reward=350.0,
    default_timesteps=5_000_000,
)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=16,
    num_learners=1,
    num_aggregator_actors_per_learner=2,
)
args = parser.parse_args()


config = (
    APPOConfig()
    .environment(StatelessCartPole)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
        env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
    )
    .learners(
        num_learners=args.num_learners,
        num_aggregator_actors_per_learner=args.num_aggregator_actors_per_learner,
    )
    .training(
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        num_epochs=1,
        vf_loss_coeff=0.05,
        entropy_coeff=0.005,
        lambda_=0.95,
        use_circular_buffer=False,
        broadcast_interval=10,
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
    run_rllib_example_script_experiment(config, args)
