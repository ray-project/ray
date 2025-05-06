"""Example of how to write a custom APPO that uses a shared, global data actor.

This example shows:

    - how to subclass an existing algorithm's (APPO) class to implement a custom
    Algorithm and override the `setup` method to control, which additional actors
    should be created (and shared) by the algo.
    - how the extra actor is created upon algorithm initialization and then given
    access to from any other actor, such as EnvRunners, AggregatorActors, and Learner
    actors.
    - how - through custom callbacks - the new actor can be written to and queried
    from anywhere within the algorithm, for example its EnvRunner actors or Learners.

We compute a plain policy gradient loss without value function baseline.
The experiment shows that even with such a simple setup, our custom algorithm is still
able to successfully learn CartPole-v1.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
"""

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_reward=250.0,
    default_iters=1000,
    default_timesteps=750000,
)
parser.set_defaults(enable_new_api_stack=True)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        APPOConfig()
        .environment("CartPole-v1")
        .callbacks(callbacks_class=)
    )

    run_rllib_example_script_experiment(base_config, args)
