"""Example of implementing and configuring a custom (torch) CNN containing RLModule.

This example:
    - demonstrates how you can subclass the TorchRLModule base class and set up your
    own CNN-stack architecture by overriding the `setup()` method.
    - shows how to override the 3 forward methods: `_forward_inference()`,
    `_forward_exploration()`, and `forward_train()` to implement your own custom forward
    logic(s). You will also learn, when each of these 3 methods is called by RLlib or
    the users of your RLModule.
    - shows how you then configure an RLlib Algorithm such that it uses your custom
    RLModule (instead of a default RLModule).

We implement a tiny CNN stack here, the exact same one that is used by the old API
stack as default CNN net. It comprises 4 convolutional layers, the last of which
ends in a 1x1 filter size and the number of filters exactly matches the number of
discrete actions (logits). This way, the (non-activated) output of the last layer only
needs to be reshaped in order to receive the policy's logit outputs. No flattening
or additional dense layer required.

The network is then used in a fast ALE/Pong-v5 experiment.


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
You should see the following output (during the experiment) in your console:

Number of trials: 1/1 (1 RUNNING)
+---------------------+----------+----------------+--------+------------------+
| Trial name          | status   | loc            |   iter |   total time (s) |
|                     |          |                |        |                  |
|---------------------+----------+----------------+--------+------------------+
| PPO_env_82b44_00000 | RUNNING  | 127.0.0.1:9718 |      1 |          98.3585 |
+---------------------+----------+----------------+--------+------------------+
+------------------------+------------------------+------------------------+
|   num_env_steps_sample |   num_env_steps_traine |   num_episodes_lifetim |
|             d_lifetime |             d_lifetime |                      e |
|------------------------+------------------------+------------------------|
|                   4000 |                   4000 |                      4 |
+------------------------+------------------------+------------------------+
"""
import gymnasium as gym

from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.rl_modules.classes.tiny_atari_cnn_rlm import TinyAtariCNN
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

parser = add_rllib_example_script_args(default_iters=100, default_timesteps=600000)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
)


if __name__ == "__main__":
    args = parser.parse_args()

    register_env(
        "env",
        lambda cfg: wrap_atari_for_new_api_stack(
            gym.make(args.env, **cfg),
            dim=42,  # <- need images to be "tiny" for our custom model
            framestack=4,
        ),
    )

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            env="env",
            env_config=dict(
                frameskip=1,
                full_action_space=False,
                repeat_action_probability=0.0,
            ),
        )
        .rl_module(
            # Plug-in our custom RLModule class.
            rl_module_spec=RLModuleSpec(
                module_class=TinyAtariCNN,
                # Feel free to specify your own `model_config` settings below.
                # The `model_config` defined here will be available inside your
                # custom RLModule class through the `self.model_config`
                # property.
                model_config={
                    "conv_filters": [
                        # num filters, kernel wxh, stride wxh, padding type
                        [16, 4, 2, "same"],
                        [32, 4, 2, "same"],
                        [256, 11, 1, "valid"],
                    ],
                },
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
