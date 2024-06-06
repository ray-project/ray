"""Example of implementing and configuring a custom (torch) RLModule.

This example:
    - demonstrates how you can subclass the TorchRLModule base class and setup your
    own neural network architecture by overriding `setup()`.
    - how to override the 3 forward methods: `_forward_inference`, `_forward_exploration`,
    and `forward_train` to implement your own custom forward logic(s). You will also learn,
    when each of these 3 methods is called by RLlib or the users of your RLModule.
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
You should see the following output (at the end of the experiment) in your console:

"""
import gymnasium as gym

from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.examples.rl_modules.classes.tiny_atari_cnn import TinyAtariCNN
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

parser = add_rllib_example_script_args(default_iters=100, default_timesteps=600000)


if __name__ == "__main__":
    args = parser.parse_args()

    register_env("env", lambda cfg: wrap_atari_for_new_api_stack(
        #TODO(sven) pull from master to get args.env
        gym.make("ALE/Pong-v5", **cfg), #args.env
        dim=42,  # <- need images to be "tiny" for our custom model
        framestack=4,
    ))

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment(
            env="env",
            env_config=dict(
                frameskip=1,
                full_action_space=False,
                repeat_action_probability=0.0,
            ),
        )
        .rl_module(
            rl_module_spec=SingleAgentRLModuleSpec(
                module_class=TinyAtariCNN,
            ),
        )
    )

    run_rllib_example_script_experiment(base_config, args)
