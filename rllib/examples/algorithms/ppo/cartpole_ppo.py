"""Example showing how to train PPO on the CartPole environment.

This example demonstrates a minimal PPO configuration for the classic CartPole
control task. CartPole is a simple benchmark where the agent must balance a
pole on a cart by moving left or right.

This example:
- Trains on the CartPole-v1 environment from Gymnasium
- Uses a small feedforward network (single hidden layer with 32 units)
- Configures shared value function layers for efficiency
- Uses tuned hyperparameters for fast convergence
- Expects to reach reward of 450.0 within 300,000 timesteps

How to run this script
----------------------
`python cartpole_ppo.py`

To run with different configuration:
`python cartpole_ppo.py --stop-reward=500.0 --stop-timesteps=500000`

To scale up with distributed learning using multiple learners and env-runners:
`python cartpole_ppo.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python cartpole_ppo.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0  --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
With the default settings, you should expect to reach a reward of ~450.0
(out of maximum 500) within 300 thousand environment timesteps.
CartPole-v1 terminates episodes at 500 steps, so rewards close to 500
indicate successful pole balancing.
"""
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_iters=200,
    default_reward=450.0,
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
    .environment("CartPole-v1")
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
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
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
            fcnet_hiddens=[32],
            fcnet_activation="linear",
        ),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
