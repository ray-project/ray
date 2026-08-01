"""Example showing how to run IMPALA on the CartPole environment.

IMPALA (Importance Weighted Actor-Learner Architecture) is a distributed
reinforcement learning algorithm that decouples acting from learning. It uses
V-trace for off-policy correction, enabling efficient training across many
distributed actors while maintaining stable learning.

This example:
    - trains on the classic CartPole-v1 control task
    - uses gradient clipping by global norm (40.0) for training stability
    - scales the learning rate with the square root of the number of learners
    - shares value function layers with the policy network for parameter efficiency
    - targets a reward of 450 (near-optimal for CartPole-v1's max of 500)

How to run this script
----------------------
`python cartpole_impala.py [options]`

To run with default settings:
`python cartpole_impala.py`

To scale up with distributed learning using multiple learners and env-runners:
`python cartpole_impala.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learner:
`python cartpole_impala.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.
By setting `--num-learners=0` and `--num-env-runners=0` will make them run locally
instead of as remote Ray Actors where breakpoints aren't possible.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the reward threshold of 450 on CartPole-v1
within 2 million timesteps (see: `default_timesteps` in the code).
CartPole-v1 has a maximum episode reward of 500, and IMPALA should
consistently achieve near-optimal performance on this task.
"""
from ray.rllib.algorithms.impala import IMPALAConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2_000_000,
)
parser.set_defaults(
    num_env_runners=4,
    num_envs_per_env_runner=16,
    num_learners=1,
)
args = parser.parse_args()


config = (
    IMPALAConfig()
    .environment("CartPole-v1")
    .env_runners(
        env_to_module_connector=lambda env, spaces, device: MeanStdFilter(),
    )
    .training(
        train_batch_size_per_learner=500,
        grad_clip=40.0,
        grad_clip_by="global_norm",
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=0.05,
        entropy_coeff=0.0,
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=True,
        ),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
