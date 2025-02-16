"""Example of using a count-based curiosity mechanism to learn in sparse-rewards envs.

This example:
    - demonstrates how to define your own count-based curiosity ConnectorV2 piece
    that computes intrinsic rewards based on simple observation counts and adds these
    intrinsic rewards to the "main" (extrinsic) rewards.
    - shows how this connector piece overrides the main (extrinsic) rewards in the
    episode and thus demonstrates how to do reward shaping in general with RLlib.
    - shows how to plug this connector piece into your algorithm's config.
    - uses Tune and RLlib to learn the env described above and compares 2
    algorithms, one that does use curiosity vs one that does not.

We use a FrozenLake (sparse reward) environment with a map size of 8x8 and a time step
limit of 14 to make it almost impossible for a non-curiosity based policy to learn.


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
from ray.rllib.connectors.env_to_module import FlattenObservations
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.connectors.classes.count_based_curiosity import (
    CountBasedCuriosity,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_reward=0.99, default_iters=200, default_timesteps=1000000
)
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--intrinsic-reward-coeff",
    type=float,
    default=1.0,
    help="The weight with which to multiply intrinsic rewards before adding them to "
    "the extrinsic ones (default is 1.0).",
)
parser.add_argument(
    "--no-curiosity",
    action="store_true",
    help="Whether to NOT use count-based curiosity.",
)

ENV_OPTIONS = {
    "is_slippery": False,
    # Use this hard-to-solve 8x8 map with lots of holes (H) to fall into and only very
    # few valid paths from the starting state (S) to the goal state (G).
    "desc": [
        "SFFHFFFH",
        "FFFHFFFF",
        "FFFHHFFF",
        "FFFFFFFH",
        "HFFHFFFF",
        "HHFHFFHF",
        "FFFHFHHF",
        "FHFFFFFG",
    ],
    # Limit the number of steps the agent is allowed to make in the env to
    # make it almost impossible to learn without (count-based) curiosity.
    "max_episode_steps": 14,
}


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            "FrozenLake-v1",
            env_config=ENV_OPTIONS,
        )
        .env_runners(
            num_envs_per_env_runner=5,
            # Flatten discrete observations (into one-hot vectors).
            env_to_module_connector=lambda env: FlattenObservations(),
        )
        .training(
            # The main code in this example: We add the `CountBasedCuriosity` connector
            # piece to our Learner connector pipeline.
            # This pipeline is fed with collected episodes (either directly from the
            # EnvRunners in on-policy fashion or from a replay buffer) and converts
            # these episodes into the final train batch. The added piece computes
            # intrinsic rewards based on simple observation counts and add them to
            # the "main" (extrinsic) rewards.
            learner_connector=(
                None if args.no_curiosity else lambda *ags, **kw: CountBasedCuriosity()
            ),
            num_epochs=10,
            vf_loss_coeff=0.01,
        )
        .rl_module(model_config=DefaultModelConfig(vf_share_layers=True))
    )

    run_rllib_example_script_experiment(base_config, args)
