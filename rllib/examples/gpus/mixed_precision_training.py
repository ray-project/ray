"""Example of using automatic mixed precision training on a torch RLModule.

This example:
  - shows how to set up an Algorithm with mixed precision training enabled through
  setting a boolean flag in the config.
  - ALTERNATIVELY: shows how users can customize the TorchLearner class to support
  mixed precision training. Note that this alternative path to the above-mentioned
  config flag serves the purpose of demonstrating the new API stack's customization
  capabilities.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack

Set the --use-custom-learner flag to switch from the convenience- and config-based
approach (in which RLlib uses its built-in mixed-precision functionality) to using the
custom TorchLearner class defined in this script here.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

Note that the shown GPU settings in this script also work in case you are not
running via tune, but instead are using the `--no-tune` command line option.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.


Results to expect
-----------------
In the console output, you should see something like this:

TODO (sven)
"""
import torch

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)


parser = add_rllib_example_script_args(
    default_iters=200, default_reward=450.0, default_timesteps=200000
)
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--use-custom-learner",
    action="store_true",
    help="Wwitches from the convenience- and config-based approach (in which RLlib "
    "uses its built-in mixed-precision functionality) to using a custom "
    "`PPOTorchLearner` class.",
)


class PPOTorchMixedPrecisionLearner(PPOTorchLearner):
    def build(self):
        super().build()
        # This example only works for single-agent (single RLModule).
        assert len(self.module) == 1 and DEFAULT_MODULE_ID in self.module

        # Create the torch gradient scaler instance to use for loss scaling.
        self._grad_scaler = torch.amp.GradScaler(self._device)

    def compute_loss_for_module(self, *args, **kwargs):
        module_loss = super().compute_loss_for_module(*args, **kwargs)
        self._grad_scaler.scale(module_loss)
        return module_loss

    def apply_gradients(self, gradients_dict):
        # Make sure the parameters do not carry gradients on their own.
        # for optim in self._optimizer_parameters:
        #    optim.zero_grad(set_to_none=True)

        # Set the gradient of the parameters.
        for pid, grad in gradients_dict.items():
            self._params[pid].grad = grad

        # Call the optimizer's step method through the grad scaler.
        optim = self.get_optimizer()
        self._grad_scaler.step(optim)
        self._grad_scaler.update()


if __name__ == "__main__":
    args = parser.parse_args()

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"
    assert args.algo == "PPO", "Must set --algo=PPO when running this script!"

    base_config = (
        PPOConfig()
        # This script only works on the new API stack.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        ).environment("CartPole-v1")
        # Use a large network to demonstrate the advantage of mixed-precision training.
        .rl_module(model_config_dict={"fcnet_hiddens": [2048, 2048, 2048, 2048]})
    )

    # Activate mixed-precision training for our torch RLModule.
    # Through the custom Learner class.
    if args.use_custom_learner:
        base_config.training(learner_class=PPOTorchMixedPrecisionLearner)
    # Or through the convenience config flag.
    else:
        base_config.experimental(_enable_torch_mixed_precision_training=True)

    run_rllib_example_script_experiment(base_config, args, keep_config=True)
