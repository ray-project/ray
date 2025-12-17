"""Example showing how to run DQN with LSTM on a partially observable environment.

This example demonstrates DQN with an LSTM network on the StatelessCartPole
environment, where cart velocity and pole angular velocity are hidden from
observations. This creates a partially observable MDP (POMDP) that requires
memory to solve effectively.

This example:
- uses the StatelessCartPole environment where velocity information is hidden,
  requiring the agent to infer dynamics from observation history
- configures an LSTM-based Q-network with max sequence length of 20 to capture
  temporal dependencies
- uses a MeanStdFilter connector for observation normalization
- applies burn-in of 8 steps to initialize LSTM hidden states before computing
  Q-values for training
- enables double DQN and dueling architecture for improved learning stability
- uses a simple EpisodeReplayBuffer with 100K capacity (not prioritized, as
  sequence sampling is more complex with LSTMs)
- schedules epsilon from 1.0 to 0.02 over 20,000 steps for exploration decay

How to run this script
----------------------
`python stateless_cartpole_dqn_with_lstm.py [options]`

To run with default settings:
`python stateless_cartpole_dqn_with_lstm.py`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 350.0 within
1,000,000 timesteps. The POMDP nature of this task makes it harder than
standard CartPole, as the agent must learn to infer velocity from position
changes over time using its LSTM memory.
"""
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=350.0,
    default_timesteps=1_000_000,
)
parser.set_defaults(
    num_env_runners=3,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

config = (
    DQNConfig()
    .environment(StatelessCartPole)
    .env_runners(
        env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
    )
    .training(
        lr=0.0005,
        train_batch_size_per_learner=32,
        replay_buffer_config={
            "type": "EpisodeReplayBuffer",
            "capacity": 100000,
        },
        n_step=1,
        double_q=True,
        dueling=True,
        num_atoms=1,
        epsilon=[(0, 1.0), (20000, 0.02)],
        burn_in_len=8,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256],
            fcnet_activation="tanh",
            fcnet_bias_initializer="zeros_",
            head_fcnet_hiddens=[256],
            head_fcnet_bias_initializer="zeros_",
            use_lstm=True,
            max_seq_len=20,
            lstm_kernel_initializer="xavier_uniform_",
        ),
    )
)

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
