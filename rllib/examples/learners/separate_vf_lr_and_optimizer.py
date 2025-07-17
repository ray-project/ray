"""Example of how to run any value function based algo (e.g. PPO) with 2 optimizers.

One optimizer (with its own learning rate and other configurations) is responsible for
updating the policy network, the other (with its own learning rate and other
configurations) for updating the value function network.

This example shows:
    - how to subclass an existing (torch) Learner and override its
    `configure_optimizers_for_module()` method.
    - how to call `Learner.register_optimizer()` from within your custom
    `configure_optimizers_for_module()` method in order to specify, which optimizer
    (type, learning rate, other settings) is responsible for which neural network
    parameters.
    - how to add custom settings (here: the additional learning rate for the
    vf-optimizer) to the `AlgorithmConfig` in order to not have to subclass and write
    your own (you could still do that, but are not required to).
    - how to plug in the custom Learner into your config and then run the
    experiment.

See the :py:class:`~ray.rllib.examples.learners.classes.separate_vf_lr_and_optimizer_learner.PPOTorchLearnerWithSeparateVfOptimizer`  # noqa
class for details on how to override the main (torch) `configure_optimizers_for_module`
function.

We assume here that the users properly sets up their RLModule to have separate policy-
and value function networks. If any model pieces are shared between the two optimizers,
you should experience learning instability up to the point where your algorithm can't
learn any useful policy anymore.


How to run this script
----------------------
`python [script file name].py --lr-vf=0.001 --lr-policy=0.0005`

Use the `--lr-policy` option to set the policy learning rate (used by the policy
optimizer) and the `--lr-vf` option to set the value function learning rate (used by the
value function optimizer).

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should expect to observe decent learning behavior from your console output:

With --lr-vf=0.0005 and --lr-policy=0.001
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_7b404_00000 | TERMINATED | 127.0.0.1:16845 |     19 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+---------------------+
|   total time (s) | num_env_steps_sampled_ | episode_return_mean |
|                  |              _lifetime |                     |
|------------------+------------------------+---------------------+
|          19.4179 |                  76000 |              459.94 |
+------------------+------------------------+---------------------+
"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.learners.classes.separate_vf_lr_and_optimizer_learner import (
    PPOTorchLearnerWithSeparateVfOptimizer,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

torch, _ = try_import_torch()


parser = add_rllib_example_script_args(default_reward=450.0)
parser.add_argument(
    "--lr-vf",
    type=float,
    default=0.0005,
    help="The learning rate used in the value function optimizer.",
)
parser.add_argument(
    "--lr-policy",
    type=float,
    default=0.001,
    help="The learning rate used in the policy optimizer.",
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
            learner_class=PPOTorchLearnerWithSeparateVfOptimizer,
            # We use this simple method here to inject a new setting that our
            # custom Learner class uses in its `configure_optimizers_for_module`
            # method. This is convenient and avoids having to subclass `PPOConfig` only
            # to add a few new settings to it. Within our Learner, we can access this
            # new setting through:
            # `self.config.learner_config_dict['lr_vf']`
            learner_config_dict={"lr_vf": args.lr_vf},
            # Some settings to make this example learn better.
            num_epochs=6,
            # Since we are using separate optimizers for the two NN components, the
            # value of `vf_loss_coeff` does not matter anymore. We set this to 1.0 here.
            vf_loss_coeff=1.0,
            # The policy learning rate, settable through the command line `--lr` arg.
            lr=args.lr_policy,
        )
        .rl_module(
            # Another very important setting is this here. Make sure you use
            # completely separate NNs for policy and value-functions.
            model_config=DefaultModelConfig(vf_share_layers=False),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
