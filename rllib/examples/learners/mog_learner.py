"""Example of how to write a custom loss function, custom rl_module and custom config.

This example shows:
    - how to subclass an existing (torch) Learner and override its `compute_loss_for_module()` method
    - how you can add your own loss to a custom (or non-custom) rl_module using compute_loss_for_module
    - how to add add custom pipeline components to a custom config class using build_learner_connector
    - how to plug in a custom Learner, rl_module, its loss, and use a custom Config to train with

See the :py:class:`~ray.rllib.examples.learners.classes.mog_loss.PPOTorchLearnerCustomMOGLoss`  # noqa
class for details on how to override the main (PPO) loss function.

This script will create a new critic network that will be a distribution of gaussians to allow for more expressivity
of the value function. The param --num_gaussians specifies how many gaussians to use for the value function. 
Normally between 3-5 generally works well.


How to run this script
----------------------
`python mog_learner.py --enable-new-api-stack --num-gaussians=3 --lr=0.001`


Results to expect
-----------------
In general with environments, the distributional critic should perform better than a normal critic given
the same hyperparameters. Some hiccups have been the alphas going too small which causes nans in the loss equation.
This has been fixed by using the log_softmax(alphas) instead of using the softmax on the alphas once out of
the module.

With --num_guassians=2 and --lr=0.0001
(trying to reach 250.0 return on CartPole in 100k env steps):
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_4a3a0_00000 | TERMINATED | 127.0.0.1:54584 |     10 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+---------------------+
|   total time (s) | num_env_steps_sampled_ | episode_return_mean |
|                  |              _lifetime |                     |
|------------------+------------------------+---------------------+
|          42.9956 |                 40_000 |              271.53 |
+------------------+------------------------+---------------------+
"""

from pprint import pprint
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.examples.learners.custom_ppo_config import CustomPPOConfig
from ray.rllib.examples.rl_modules.classes.custom_mog import MOGModule
from ray.rllib.examples.learners.classes.mog_loss import PPOTorchLearnerCustomMOGLoss
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

torch, _ = try_import_torch()


parser = add_rllib_example_script_args(
    default_reward=250.0,
    default_timesteps=200_000,
)
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--num_gaussians",
    type=int,
    default=3,
    help="Number of gaussians to represent the value function",
)
parser.add_argument(
    "--lr",
    type=float,
    default=0.0003,
    help="Learning rate",
)

if __name__ == "__main__":
    args = parser.parse_args()

    custom_config = {
    "vf_share_layers": False,
    "fcnet_hiddens": [128, 128],
    "fcnet_activation": "LeakyReLU",
    "num_mixture_components": args.num_gaussians,
    }

    module_to_load_spec = RLModuleSpec(
        module_class=MOGModule,
        model_config=custom_config,
    )

    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"
    assert args.algo == "PPO", "Must set --algo=PPO when running this script!"

    base_config = (
        CustomPPOConfig()
        .environment("CartPole-v1")
        .training(
            # set the learner class which will use the custom loss function we want
            learner_class=PPOTorchLearnerCustomMOGLoss,
            # can pass args through the learner_config_dict to have access in the custom loss config
            lr=args.lr,
        )
        .rl_module(
            rl_module_spec=module_to_load_spec,
        )
    )

algo = base_config.build()
total_timesteps = 0
default_timesteps = 200_000
default_reward = 250
for iteration in range(1000):
    result = algo.train()

    pprint(result)
    
    episode_return_mean = result['env_runners']['episode_return_mean']
    total_timesteps = result['env_runners']['num_env_steps_sampled_lifetime']

    print(f"Iteration {iteration}: Episode Return Mean: {episode_return_mean}, Total Timesteps: {total_timesteps}")
    if episode_return_mean >= default_reward:
        print(f"Stopping: Reached target reward of {default_reward} at iteration {iteration}")
        break
    if total_timesteps >= default_timesteps:
        print(f"Stopping: Reached maximum timesteps of {default_timesteps} at iteration {iteration}")
        break
# run_rllib_example_script_experiment(base_config, args)