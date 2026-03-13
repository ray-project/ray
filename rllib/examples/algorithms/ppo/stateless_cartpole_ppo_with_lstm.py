"""Example showing how to train PPO with LSTM on a partially observable environment.

This example demonstrates using an LSTM-based policy network to handle partial
observability. The StatelessCartPole environment removes velocity information
from observations, requiring the agent to infer dynamics from observation history.

This example:
- Trains on StatelessCartPole (CartPole without velocity observations)
- Uses an LSTM-based policy network (use_lstm=True) with max sequence length of 20
- Applies MeanStdFilter connector for observation normalization
- Uses shared value function layers (vf_share_layers=True)
- Expects to reach reward of 350.0 within 5 million timesteps

How to run this script
----------------------
`python stateless_cartpole_ppo_with_lstm.py`

The script defaults to using 4 env-runners for parallel sample collection.

To run with different configuration:
`python stateless_cartpole_ppo_with_lstm.py --stop-reward=400.0 --stop-timesteps=3000000`

To scale up with distributed learning using multiple learners and env-runners:
`python stateless_cartpole_ppo_with_lstm.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python stateless_cartpole_ppo_with_lstm.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
With the default settings, you should expect to reach a reward of ~350.0
within approximately 300 thousand environment timesteps. The LSTM allows the
agent to infer the hidden velocity state from the history of position
observations. The learning rate is scaled with the square root of the number
of learners for distributed training.
"""
from ray.rllib.algorithms.ppo import PPOConfig
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
    default_timesteps=300_000,
)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=16,
    num_learners=1,
)
args = parser.parse_args()


config = (
    PPOConfig()
    .environment(StatelessCartPole)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
        env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
        rollout_fragment_length=32,
    )
    .learners(
        num_learners=args.num_learners,
    )
    .training(
        train_batch_size=2048,
        minibatch_size=128,
        lr=0.0003,
        num_epochs=5,
        vf_loss_coeff=1.5,
        entropy_coeff=0,
        gamma=0.99,
        lambda_=0.95,
        clip_param=0.3,
        vf_clip_param=10_000,
        use_kl_loss=False,
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
