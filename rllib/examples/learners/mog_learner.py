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
`python [script file name].py --enable-new-api-stack --regularizer-coeff=0.02
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

With --num_guassians=2 and --lr=0.0001
(trying to reach 250.0 return on CartPole in 100k env steps):
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_4a3a0_00000 | TERMINATED | 127.0.0.1:54584 |     56 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+---------------------+
|   total time (s) | num_env_steps_sampled_ | episode_return_mean |
|                  |              _lifetime |                     |
|------------------+------------------------+---------------------+
|          96.8559 |                 112000 |              251.51 |
+------------------+------------------------+---------------------+
"""

from ray.rllib.examples.learners.custom_ppo_config import CustomPPOConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.examples.rl_modules.classes.custom_mog import (
    MOGModule,
)
from ray.rllib.examples.learners.classes.mog_loss import (
    PPOTorchLearnerCustomMOGLoss,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog

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
            # learner_class=PPOTorchLearnerCustomMOGLoss,
            # can pass args through the learner_config_dict to have access in the custom loss config
            learner_config_dict={"num_gaussians": args.num_gaussians},
            # Some settings to make this example learn better.
            num_epochs=15,
            vf_loss_coeff=1.0,
            clip_param=0.2,
            grad_clip_by='norm',
            lambda_=0.95,
            gamma=0.99,
            grad_clip=1.0,
            train_batch_size=2_000,
            minibatch_size=400,
            # vf_clip_param=10.0,
            lr=args.lr,
        )
        .rl_module(
            rl_module_spec=module_to_load_spec,
        )
        # .learners(
        #     num_learners=5,
        #     num_cpus_per_learner=1,
            # num_gpus_per_learner=0.1, # num_gpus old stack (which would be num_gpus=2 for this case)
        # )
        # .resources(
        #     num_cpus_for_main_process=1, # default (replaced num_cpus_for_local_worker)
        # )
        # .rollouts(
        # num_env_runners=2,
        # )
    )

    run_rllib_example_script_experiment(base_config, args)