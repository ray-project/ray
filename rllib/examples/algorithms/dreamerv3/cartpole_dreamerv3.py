"""Example showing how to run DreamerV3 on the CartPole-v1 environment.

This is a minimal example demonstrating DreamerV3 on the classic CartPole
control task. CartPole is a simple discrete-action environment where the agent
must balance a pole on a moving cart. This example uses the smallest model
configuration ("XS") and a high training ratio, making it suitable for quick
experimentation and debugging.

This example:
    - Runs DreamerV3 on CartPole-v1 with vector observations (not pixels)
    - Uses the "XS" model size for fast training on this simple task
    - Configures a high training ratio of 1024 (gradient steps per env step)
    - Provides a minimal configuration suitable for testing and experimentation
    - Expects to solve CartPole (reward ~500) quickly due to the simple task

How to run this script
----------------------
`python cartpole_dreamerv3.py [options]`

To run with default settings:
`python cartpole_dreamerv3.py`

To scale up with distributed learning using multiple learners and env-runners:
`python cartpole_dreamerv3.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python cartpole_dreamerv3.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
CartPole-v1 should be solved rapidly (achieving rewards near 500) due to its
simplicity. The "XS" model size and high training ratio (1024) allow the world
model to quickly learn the CartPole dynamics. This configuration is ideal for
verifying that DreamerV3 is working correctly before scaling to harder tasks.
"""
from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=10_000_000,
)
args = parser.parse_args()

config = (
    DreamerV3Config()
    .environment("CartPole-v1")
    .training(
        model_size="XS",
        training_ratio=1024,
    )
)

if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
