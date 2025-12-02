"""Example of logging custom metrics inside the `Algorithm.training_step()` method.

RLlib provides a MetricsLogger instance inside most components of an Algorithm,
including the Algorithm itself.

This example:
- Shows how to subclass a custom Algorithm class (VPG) and override its
`training_step()` method.
- Shows how to use the MetricsLogger instance of Algorithm to log the ratio between
the time spent on sampling over the time spent on the learning update. For on-policy
algorithms, this ratio indicates, where scaling would yield the largest speedups, on
the EnvRunner side or on the Learner side.
- Shows how to access the logged metrics at the end of an iteration through the returned
result dict and how to assert, the new metrics has been properly logged.


How to run this script
----------------------
`python [script file name].py --wandb-key [your WandB key]
--wandb-project [some project name]`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see something similar to the below on your terminal when running
this script:

+-----------------------------------------------+------------+--------+
| Trial name                                    | status     |   iter |
|                                               |            |        |
|-----------------------------------------------+------------+--------+
| MyVPGWithExtraMetrics_CartPole-v1_d2c5c_00000 | TERMINATED |    100 |
+-----------------------------------------------+------------+--------+
+------------------+-----------------------+------------------------+
|   total time (s) |   episode_return_mean |   ratio_time_sampling_ |
|                  |                       |          over_learning |
|------------------+-----------------------+------------------------|
|          10.0308 |                 50.91 |                4.84769 |
+------------------+-----------------------+------------------------+

Found logged `ratio_time_sampling_over_learning` in result dict.
"""
from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.examples.algorithms.classes.vpg import VPG, VPGConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ENV_RUNNER_SAMPLING_TIMER,
    LEARNER_UPDATE_TIMER,
    TIMERS,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


class MyVPGWithExtraMetrics(VPG):
    @override(VPG)
    def training_step(self) -> None:
        # Call the actual VPG training_step.
        super().training_step()

        # Look up some already logged metrics through the `peek()` method.
        time_spent_on_sampling = self.metrics.peek((TIMERS, ENV_RUNNER_SAMPLING_TIMER))
        time_spent_on_learning = self.metrics.peek((TIMERS, LEARNER_UPDATE_TIMER))

        # Log extra metrics, still for this training step.
        self.metrics.log_value(
            "ratio_time_sampling_over_learning",
            time_spent_on_sampling / time_spent_on_learning,
        )

    @classmethod
    @override(VPG)
    def get_default_config(cls) -> AlgorithmConfig:
        return VPGConfig(algo_class=cls)


parser = add_rllib_example_script_args(default_reward=50.0)
parser.set_defaults(default_timesteps=50000)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = MyVPGWithExtraMetrics.get_default_config().environment("CartPole-v1")

    results = run_rllib_example_script_experiment(base_config, args)

    # Check, whether the logged metrics are present.
    if args.no_tune:
        assert "ratio_time_sampling_over_learning" in results
    else:
        assert "ratio_time_sampling_over_learning" in results[0].metrics

    print("Found logged `ratio_time_sampling_over_learning` in result dict.")
