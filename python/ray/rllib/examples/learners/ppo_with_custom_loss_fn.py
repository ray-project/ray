"""Example of how to write a custom loss function (based on the existing PPO loss).

This example shows:
    - how to subclass an existing (torch) Learner and override its
    `compute_loss_for_module()` method.
    - how you can add your own loss terms to the subclassed "base loss", in this
    case here a weights regularizer term with the intention to keep the learnable
    parameters of the RLModule reasonably small.
    - how to add custom settings (here: the regularizer coefficient) to the
    `AlgorithmConfig` in order to not have to subclass and write your own
    (you could still do that, but are not required to).
    - how to plug in the custom Learner into your config and then run the
    experiment.

See the :py:class:`~ray.rllib.examples.learners.classes.custom_loss_fn_learner.PPOTorchLearnerWithWeightRegularizerLoss`  # noqa
class for details on how to override the main (PPO) loss function.

We compute a naive regularizer term averaging over all parameters of the RLModule and
add this mean value (multiplied by the regularizer coefficient) to the base PPO loss.
The experiment shows that even with a large learning rate, our custom Learner is still
able to learn properly as it's forced to keep the weights small.


How to run this script
----------------------
`python [script file name].py --regularizer-coeff=0.02
--lr=0.01`

Use the `--regularizer-coeff` option to set the value of the coefficient with which
the mean NN weight is being multiplied (inside the total loss) and the `--lr` option
to set the learning rate. Experiments using a large learning rate and no regularization
(`--regularizer-coeff=0.0`) should NOT learn a decently working policy.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see that - given a large learning rate - only with
weight regularization (`--regularizer-coeff` > 0.0), the algo has a chance to learn
a decent policy:

With --regularizer-coeff=0.02 and --lr=0.01
(trying to reach 250.0 return on CartPole in 100k env steps):
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_4a3a0_00000 | TERMINATED | 127.0.0.1:16845 |     18 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+---------------------+
|   total time (s) | num_env_steps_sampled_ | episode_return_mean |
|                  |              _lifetime |                     |
|------------------+------------------------+---------------------+
|          16.8842 |                  72000 |              256.35 |
+------------------+------------------------+---------------------+

With --regularizer-coeff=0.0 and --lr=0.01
(trying to reach 250.0 return on CartPole in 100k env steps):

[HAS SIGNIFICANT PROBLEMS REACHING THE DESIRED RETURN]
"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.learners.classes.custom_ppo_loss_fn_learner import (
    PPOTorchLearnerWithWeightRegularizerLoss,
)
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


parser = add_rllib_example_script_args(
    default_reward=250.0,
    default_timesteps=200000,
)
parser.add_argument(
    "--regularizer-coeff",
    type=float,
    default=0.02,
    help="The coefficient with which to multiply the mean NN-weight by (and then add "
    "the result of this operation to the main loss term).",
)
parser.add_argument(
    "--lr",
    type=float,
    default=0.01,
    help="The learning rate to use.",
)


if __name__ == "__main__":
    args = parser.parse_args()

    assert args.algo == "PPO", "Must set --algo=PPO when running this script!"

    base_config = (
        PPOConfig()
        .environment("CartPole-v1")
        .training(
            # This is the most important setting in this script: We point our PPO
            # algorithm to use the custom Learner (instead of the default
            # PPOTorchLearner).
            learner_class=PPOTorchLearnerWithWeightRegularizerLoss,
            # We use this simple method here to inject a new setting that our
            # custom Learner class uses in its loss function. This is convenient
            # and avoids having to subclass `PPOConfig` only to add a few new settings
            # to it. Within our Learner, we can access this new setting through:
            # `self.config.learner_config_dict['regularizer_coeff']`
            learner_config_dict={"regularizer_coeff": args.regularizer_coeff},
            # Some settings to make this example learn better.
            num_epochs=6,
            vf_loss_coeff=0.01,
            # The learning rate, settable through the command line `--lr` arg.
            lr=args.lr,
        )
        .rl_module(
            model_config=DefaultModelConfig(vf_share_layers=True),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
