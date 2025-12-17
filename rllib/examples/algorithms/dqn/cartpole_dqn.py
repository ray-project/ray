"""Example showing how to run DQN on the CartPole environment.

This example demonstrates DQN (Deep Q-Network) with modern enhancements on the
classic CartPole-v1 control task. CartPole is a simple environment where the
goal is to balance a pole on a moving cart by applying left/right forces.

This example:
- uses the CartPole-v1 environment with discrete actions (left/right)
- configures prioritized experience replay with 50K capacity buffer (alpha=0.6,
  beta=0.4) for more efficient learning from important transitions
- enables double DQN to reduce overestimation bias in Q-value estimates
- uses dueling network architecture to separately estimate state value and
  action advantages
- applies variable n-step returns (randomly sampling between 2-5 steps) for
  improved credit assignment
- schedules epsilon from 1.0 to 0.02 over 10,000 steps for exploration decay
- scales learning rate with sqrt of number of learners for multi-GPU training

How to run this script
----------------------
`python cartpole_dqn.py [options]`

To run with default settings:
`python cartpole_dqn.py`

To scale up with multiple learners:
`python cartpole_dqn.py --num-learners=2`

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
The algorithm should reach the default reward threshold of 450.0 within
200,000 timesteps. CartPole-v1 has a maximum episode length of 500 steps,
so rewards near 500 indicate near-optimal pole balancing.
"""
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=200_000,
)
args = parser.parse_args()

config = (
    DQNConfig()
    .environment(env="CartPole-v1")
    .training(
        lr=0.0005 * (args.num_learners or 1) ** 0.5,
        train_batch_size_per_learner=32,
        replay_buffer_config={
            "type": "PrioritizedEpisodeReplayBuffer",
            "capacity": 50_000,
            "alpha": 0.6,
            "beta": 0.4,
        },
        n_step=(2, 5),
        double_q=True,
        dueling=True,
        epsilon=[(0, 1.0), (10_000, 0.02)],
    )
    .rl_module(
        model_config=DefaultModelConfig(
            fcnet_hiddens=[256],
            fcnet_activation="tanh",
            fcnet_bias_initializer="zeros_",
            head_fcnet_hiddens=[256],
            head_fcnet_bias_initializer="zeros_",
        ),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
