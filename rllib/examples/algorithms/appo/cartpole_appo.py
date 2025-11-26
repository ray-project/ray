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
`python [script file name].py [options]`

To run with default settings:
`python [script file name].py`

To scale up with distributed training:
`python [script file name].py --num-env-runners=4 --num-learners=2`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
The algorithm should reach the default reward threshold of 450.0 within
approximately 1 million timesteps. Training is fast on this simple environment,
typically converging in under a minute on a single machine. The low value
function loss coefficient (0.05) and zero entropy coefficient work well for
this deterministic environment.
"""
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=250_000,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()


config = (
    APPOConfig()
    .environment("CartPole-v1")
    .training(
        circular_buffer_iterations_per_batch=2,
        vf_loss_coeff=0.05,
        entropy_coeff=0.0,
    )
    .rl_module(
        model_config=DefaultModelConfig(vf_share_layers=True),
    )
)

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
