"""Example of how to write a custom Algorithm.

This is an end-to-end example for how to implement a custom Algorithm, including
a matching AlgorithmConfig class and Learner class. There is no particular RLModule API
needed for this algorithm, which means that any TorchRLModule returning actions
or action distribution parameters suffices.

The RK algorithm implemented here is "vanilla policy gradient" (VPG) in its simplest
form, without a value function baseline.

See the actual VPG algorithm class here:
https://github.com/ray-project/ray/blob/master/rllib/examples/algorithms/classes/vpg.py

The Learner class the algorithm uses by default (if the user doesn't specify a custom
Learner):
https://github.com/ray-project/ray/blob/master/rllib/examples/learners/classes/vpg_torch_learner.py  # noqa

And the RLModule class the algorithm uses by default (if the user doesn't specify a
custom RLModule):
https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/classes/vpg_torch_rlm.py  # noqa

This example shows:
    - how to subclass the AlgorithmConfig base class to implement a custom algorithm's.
    config class.
    - how to subclass the Algorithm base class to implement a custom Algorithm,
    including its `training_step` method.
    - how to subclass the TorchLearner base class to implement a custom Learner with
    loss function, overriding `compute_loss_for_module` and
    `after_gradient_based_update`.
    - how to define a default RLModule used by the algorithm in case the user
    doesn't bring their own custom RLModule. The VPG algorithm doesn't require any
    specific RLModule APIs, so any RLModule returning actions or action distribution
    inputs suffices.

We compute a plain policy gradient loss without value function baseline.
The experiment shows that even with such a simple setup, our custom algorithm is still
able to successfully learn CartPole-v1.


How to run this script
----------------------
`python [script file name].py`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
With some fine-tuning of the learning rate, the batch size, and maybe the
number of env runners and number of envs per env runner, you should see decent
learning behavior on the CartPole-v1 environment:

+-----------------------------+------------+--------+------------------+
| Trial name                  | status     |   iter |   total time (s) |
|                             |            |        |                  |
|-----------------------------+------------+--------+------------------+
| VPG_CartPole-v1_2973e_00000 | TERMINATED |    451 |          59.5184 |
+-----------------------------+------------+--------+------------------+
+-----------------------+------------------------+------------------------+
|   episode_return_mean |   num_env_steps_sample |   ...env_steps_sampled |
|                       |             d_lifetime |   _lifetime_throughput |
|-----------------------+------------------------+------------------------|
|                250.52 |                 415787 |                7428.98 |
+-----------------------+------------------------+------------------------+
"""

from ray.rllib.examples.algorithms.classes.vpg import VPGConfig
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_reward=250.0,
    default_iters=1000,
    default_timesteps=1_000_000,
)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        VPGConfig()
        .environment("CartPole-v1")
        .training(
            # The only VPG-specific setting. How many episodes per train batch?
            num_episodes_per_train_batch=10,
            # Set other config parameters.
            lr=0.0005,
            # Note that you don't have to set any specific Learner class, because
            # our custom Algorithm already defines the default Learner class to use
            # through its `get_default_learner_class` method, which returns
            # `VPGTorchLearner`.
            # learner_class=VPGTorchLearner,
        )
        # Increase the number of EnvRunners (default is 1 for VPG)
        # or the number of envs per EnvRunner.
        .env_runners(num_env_runners=2, num_envs_per_env_runner=1)
        # Plug in your own RLModule class. VPG doesn't require any specific
        # RLModule APIs, so any RLModule returning `actions` or `action_dist_inputs`
        # from the forward methods works ok.
        # .rl_module(
        #    rl_module_spec=RLModuleSpec(module_class=...),
        # )
    )

    run_rllib_example_script_experiment(base_config, args)
