"""Simple example showing how to run APPO on CartPole-v1.

This example demonstrates the APPO (Asynchronous Proximal Policy Optimization)
algorithm on the classic CartPole-v1 environment. APPO is a distributed,
off-policy variant of PPO that uses a circular replay buffer and supports
asynchronous training, making it more sample-efficient than on-policy PPO.

The configuration here uses shared layers between the policy and value function
networks (`vf_share_layers=True`), which is efficient for simple environments
like CartPole. The circular buffer is configured with 2 iterations per batch,
allowing the algorithm to reuse collected experiences multiple times.

How to run this script
----------------------
`python cartpole_appo.py [options]`

To run with default settings:
`python cartpole_appo.py`

To scale up with distributed learning using multiple learners and env-runners:
`python cartpole_appo.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python cartpole_appo.py --num-learners=1 --num-gpus-per-learner=1`

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
The algorithm should reach the default reward threshold of 450.0 within
approximately 4 million timesteps (see: `default_timesteps` in the code).
The number of environment steps can be
through changed `default_timesteps`. Training is fast on this simple environment,
typically converging in under a minute on a single machine. The low value
function loss coefficient (0.05) and zero entropy coefficient work well for
this deterministic environment.
"""
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_iters=200,
    default_reward=450.0,
    default_timesteps=4_000_000,
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
    .environment("CartPole-v1")
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
    )
    .learners(
        num_learners=args.num_learners,
        num_aggregator_actors_per_learner=args.num_aggregator_actors_per_learner,
    )
    .training(
        circular_buffer_iterations_per_batch=2,
        vf_loss_coeff=0.05,
        entropy_coeff=0.0,
        lambda_=0.95,
    )
    .rl_module(
        model_config=DefaultModelConfig(vf_share_layers=True),
    )
)

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
