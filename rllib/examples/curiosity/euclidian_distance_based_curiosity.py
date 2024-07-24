"""Example of a euclidian-distance curiosity mechanism to learn in sparse-rewards envs.

This example:
    - demonstrates how to define your own euclidian-distance-based curiosity ConnectorV2
    piece that computes intrinsic rewards based on the delta between incoming
    observations and some set of already stored (prior) observations. Thereby, the
    further away the incoming observation is from the already stored ones, the higher
    its corresponding intrinsic reward.
    - shows how this connector piece adds the intrinsic reward to the corresponding
    "main" (extrinsic) reward and overrides the value in the "rewards" key in the
    episode. It thus demonstrates how to do reward shaping in general with RLlib.
    - shows how to plug this connector piece into your algorithm's config.
    - uses Tune and RLlib to learn the env described above and compares 2
    algorithms, one that does use curiosity vs one that does not.

We use the MountainCar-v0 environment, a sparse-reward env that is very hard to learn
for a regular PPO algorithm.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

Use the `--no-curiosity` flag to disable curiosity learning and force your policy
to be trained on the task w/o the use of intrinsic rewards. With this option, the
algorithm should NOT succeed.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see that only a PPO policy that uses curiosity can
actually learn.

Policy using count-based curiosity:
+-------------------------------+------------+--------+------------------+
| Trial name                    | status     |   iter |   total time (s) |
|                               |            |        |                  |
|-------------------------------+------------+--------+------------------+
| PPO_FrozenLake-v1_109de_00000 | TERMINATED |     48 |            44.46 |
+-------------------------------+------------+--------+------------------+
+------------------------+-------------------------+------------------------+
|    episode_return_mean |   num_episodes_lifetime |   num_env_steps_traine |
|                        |                         |             d_lifetime |
|------------------------+-------------------------+------------------------|
|                   0.99 |                   12960 |                 194000 |
+------------------------+-------------------------+------------------------+

Policy NOT using curiosity:
[DOES NOT LEARN AT ALL]
"""
from ray.rllib.connectors.env_to_module import MeanStdFilter
from ray.rllib.examples.connectors.classes.euclidian_distance_based_curiosity import (
    EuclidianDistanceBasedCuriosity,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

# TODO (sven): SB3's PPO does seem to learn MountainCar-v0 until a reward of ~-110.
#  We might have to play around some more with different initializations, more
#  randomized SGD minibatching (we don't shuffle batch rn), etc.. to get to these
#  results as well.
parser = add_rllib_example_script_args(
    default_reward=-130.0, default_iters=2000, default_timesteps=1000000
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_env_runners=4,
)
parser.add_argument(
    "--intrinsic-reward-coeff",
    type=float,
    default=0.0001,
    help="The weight with which to multiply intrinsic rewards before adding them to "
    "the extrinsic ones (default is 0.0001).",
)
parser.add_argument(
    "--no-curiosity",
    action="store_true",
    help="Whether to NOT use count-based curiosity.",
)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("MountainCar-v0")
        .env_runners(
            env_to_module_connector=lambda env: MeanStdFilter(),
            num_envs_per_env_runner=5,
        )
        .training(
            # The main code in this example: We add the
            # `EuclidianDistanceBasedCuriosity` connector piece to our Learner connector
            # pipeline. This pipeline is fed with collected episodes (either directly
            # from the EnvRunners in on-policy fashion or from a replay buffer) and
            # converts these episodes into the final train batch. The added piece
            # computes intrinsic rewards based on simple observation counts and add them
            # to the "main" (extrinsic) rewards.
            learner_connector=(
                None
                if args.no_curiosity
                else lambda *ags, **kw: EuclidianDistanceBasedCuriosity()
            ),
            # train_batch_size_per_learner=512,
            grad_clip=20.0,
            entropy_coeff=0.003,
            gamma=0.99,
            lr=0.0002,
            lambda_=0.98,
        )
        # .rl_module(model_config_dict={"fcnet_activation": "relu"})
    )

    run_rllib_example_script_experiment(base_config, args)
