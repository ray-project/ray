"""Example on how to define and run an experiment with a custom action distribution.

The example uses an additional `temperature` parameter on top of the built-in
`TorchCategorical` class. Incoming logits (outputs from the RLModule) are divided by
this temperature before creating the underlying
torch.distributions.categorical.Categorical object.

This examples:
    - Shows how to write a custom RLlib action distribution class accepting an
    additional parameter in its constructor.
    - demonstrates how you can subclass the TorchRLModule base class and write your
    own architecture by overriding the `setup()` method.
    - shows how to set the attribute `self.action_dist_cls` in that same `setup()`
    method. For an alternative way of defining action distribution classes for your
    RLModules, see the `setup()` method implementation in the imported
    `CustomActionDistributionRLModule` class.
    - shows how you then configure an RLlib Algorithm such that it uses your custom
    RLModule (instead of a default RLModule).


How to run this script
----------------------
`python [script file name].py --temperature=0.8`

Use the `--temperature` setting to change the temperature. Higher values (>>1.0) lead
to almost random behavior, lower values (<<1.0) lead to always-greedy behavior. Note
though, that both extremes hurt learning performance.

Control the number of `EnvRunner`s with the `--num-env-runners` flag. This
will increase the sampling speed.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
With a --temperature setting of 0.75, learning seems to be particularly easy with the
given other parameters:

+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_1bbe0_00000 | TERMINATED | 127.0.0.1:81594 |     22 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |    episode_return_mean |   num_env_steps_sample |
|                  |                        |             d_lifetime |
|------------------+------------------------+------------------------|
|          17.6368 |                 450.54 |                  88000 |
+------------------+------------------------+------------------------+
"""

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.rl_modules.classes.custom_action_distribution_rlm import (
    CustomActionDistributionRLModule,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_timesteps=200000,
    default_reward=450.0,
)
parser.add_argument(
    "--temperature",
    type=float,
    default=2.0,
    help="The action distribution temperature to apply to the raw model logits. "
    "Logits are first divided by the temperature, then an underlying torch.Categorical "
    "distribution is created from those altered logits and used for sampling actions. "
    "Set this to <<1.0 to approximate greedy behavior and to >>1.0 to approximate "
    "random behavior.",
)


if __name__ == "__main__":
    args = parser.parse_args()

    if args.algo != "PPO":
        raise ValueError(
            "This example script only runs with PPO! Set --algo=PPO on the command "
            "line."
        )

    base_config = (
        PPOConfig()
        .environment("CartPole-v1")
        .training(
            lr=0.0003,
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
        # Specify the RLModule class to be used.
        .rl_module(
            rl_module_spec=RLModuleSpec(
                module_class=CustomActionDistributionRLModule,
                model_config={
                    "hidden_dim": 128,
                    "action_dist_temperature": args.temperature,
                },
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
