"""Example of using GPUs on the EnvRunners (b/c Env and/or RLModule require these).

The number of GPUs required, just for your EnvRunners (excluding those needed for
training your RLModule) can be computed by:
`num_gpus = config.num_env_runners * config.num_gpus_per_env_runner`

This example:
  - shows how to write an Env that uses the GPU.
  - shows how to configure your algorithm such that it allocates any number of GPUs
  (including fractional < 1.0) to each (remote) EnvRunner worker.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-env_runners=
[number of EnvRunners, e.g. 2] --num-gpus-per-env-runner [int or some fraction <1.0]`

The following command line combinations been tested on a 4 NVIDIA T4 GPUs (16 vCPU)
machine.
TODO (sven): Fix these
Note that for each run, 4 tune trials will be setup; see tune.grid_search over 4
learning rates in the `base_config` below:
1) --num-learners=1 --num-gpus-per-learner=0.5 (2.0 GPUs used).
2) --num-learners=1 --num-gpus-per-learner=0.3 (1.2 GPUs used).
3) --num-learners=1 --num-gpus-per-learner=0.25 (1.0 GPU used).
4) non-sensical setting: --num-learners=2 --num-gpus-per-learner=0.5 (expect an
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

"""
from ray.rllib.examples.envs.classes.gpu_requiring_env import GPURequiringEnv
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_iters=50, default_reward=0.9, default_timesteps=100000
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_env_runners=2,
)
parser.add_argument("--num-gpus-per-env-runner", type=float, default=0.5)


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment(GPURequiringEnv)
        # Define Learner scaling.
        .env_runners(
            # How many EnvRunner workers do we need?
            num_env_runners=args.num_env_runners,
            # How many GPUs does each EnvRunner require? Note that the memory on (a
            # possibly fractional GPU) must be enough to accommodate the RLModule AND
            # if applicable also the Env's GPU needs).
            num_gpus_per_env_runner=args.num_gpus_per_env_runner,
        )
    )

    run_rllib_example_script_experiment(base_config, args)
