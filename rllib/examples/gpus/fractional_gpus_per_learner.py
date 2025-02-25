"""Example of using fractional GPUs (< 1.0) per Learner worker.

The number of GPUs required, just for learning (excluding those maybe needed on your
EnvRunners, if applicable) can be computed by:
`num_gpus = config.num_learners * config.num_gpus_per_learner`

This example:
  - shows how to set up an Algorithm that uses one or more Learner workers ...
  - ... and how to assign a fractional (< 1.0) number of GPUs to each of these Learners.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-learners=
[number of Learners, e.g. 1] --num-gpus-per-learner [some fraction <1.0]`

The following command line combinations been tested on a 4 NVIDIA T4 GPUs (16 vCPU)
machine.
Note that for each run, 4 tune trials will be setup; see tune.grid_search over 4
learning rates in the `base_config` below:
1) --num-learners=1 --num-gpus-per-learner=0.5 (2.0 GPUs used).
2) --num-learners=1 --num-gpus-per-learner=0.3 (1.2 GPUs used).
3) --num-learners=1 --num-gpus-per-learner=0.25 (1.0 GPU used).
4) --num-learners=2 --num-gpus-per-learner=1 (8 GPUs used).
5) non-sensical setting: --num-learners=2 --num-gpus-per-learner=0.5 (expect an
NCCL-related error due to the fact that torch will try to perform DDP sharding,
but notices that the shards sit on the same GPU).

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
In the console output, you can see that only fractional GPUs are being used by RLlib:

== Status ==
...
Logical resource usage: 12.0/16 CPUs, 1.0/4 GPUs (...)
...
Number of trials: 4/4 (4 RUNNING)

The final output should look something like this:
+-----------------------------+------------+-----------------+--------+--------+
| Trial name                  | status     | loc             |     lr |   iter |
|                             |            |                 |        |        |
|-----------------------------+------------+-----------------+--------+--------+
| PPO_CartPole-v1_7104b_00000 | TERMINATED | 10.0.0.39:31197 | 0.005  |     10 |
| PPO_CartPole-v1_7104b_00001 | TERMINATED | 10.0.0.39:31202 | 0.003  |     11 |
| PPO_CartPole-v1_7104b_00002 | TERMINATED | 10.0.0.39:31203 | 0.001  |     10 |
| PPO_CartPole-v1_7104b_00003 | TERMINATED | 10.0.0.39:31204 | 0.0001 |     11 |
+-----------------------------+------------+-----------------+--------+--------+

+----------------+----------------------+----------------------+----------------------+
| total time (s) | num_env_steps_sample | num_env_steps_traine | num_episodes_lifetim |
|                |           d_lifetime |           d_lifetime |                    e |
|----------------+----------------------+----------------------+----------------------|
|        101.002 |                40000 |                40000 |                  346 |
|        110.03  |                44000 |                44000 |                  395 |
|        101.171 |                40000 |                40000 |                  328 |
|        110.091 |                44000 |                44000 |                  478 |
+----------------+----------------------+----------------------+----------------------+
"""
from ray import tune
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_iters=50, default_reward=180, default_timesteps=100000
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_env_runners=2,
)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # This script only works on the new API stack.
        .api_stack(
            enable_rl_module_and_learner=True,
            enable_env_runner_and_connector_v2=True,
        )
        .environment("CartPole-v1")
        # Define EnvRunner scaling.
        .env_runners(num_env_runners=args.num_env_runners)
        # Define Learner scaling.
        .learners(
            # How many Learner workers do we need? If you have more than 1 GPU,
            # set this parameter to the number of GPUs available.
            num_learners=args.num_learners,
            # How many GPUs does each Learner need? If you have more than 1 GPU or only
            # one Learner, you should set this to 1, otherwise, set this to some
            # fraction.
            num_gpus_per_learner=args.num_gpus_per_learner,
        )
        # 4 tune trials altogether.
        .training(lr=tune.grid_search([0.005, 0.003, 0.001, 0.0001]))
    )

    run_rllib_example_script_experiment(base_config, args, keep_config=True)
