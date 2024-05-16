"""Example of using fractional GPUs (< 1.0) per Learner worker.

This example:
  - shows how to setup an Algorithm that uses one or more Learner workers ...
  - ... and assigns a fractional (< 1.0) number of GPUs to each of these Learners.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-learner-workers=
[number of Learner workers, e.g. 1] --num-gpus-per-learner [some fraction <1.0]`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

You can visualize experiment results in ~/ray_results using TensorBoard.
"""
from ray import tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(
    default_iters=50, default_reward=180, default_timesteps=100000
)
parser.add_argument("--num-learners", type=int, default=1)
parser.add_argument("--num-gpus-per-learner", type=float, default=0.5)


if __name__ == "__main__":
    args = parser.parse_args()

    # These configs have been tested on a p2.8xlarge machine (8 GPUs, 16 CPUs),
    # where ray was started using only one of these GPUs:
    # $ ray start --num-gpus=1 --head

    # Tested arg combinations (4 tune trials will be setup; see
    # tune.grid_search over 4 learning rates below):
    # - num_gpus=0.5 (2 tune trials should run in parallel).
    # - num_gpus=0.3 (3 tune trials should run in parallel).
    # - num_gpus=0.25 (4 tune trials should run in parallel)
    # - num_gpus=0.2 + num_gpus_per_worker=0.1 (1 worker) -> 0.3
    #   -> 3 tune trials should run in parallel.
    # - num_gpus=0.2 + num_gpus_per_worker=0.1 (2 workers) -> 0.4
    #   -> 2 tune trials should run in parallel.
    # - num_gpus=0.4 + num_gpus_per_worker=0.1 (2 workers) -> 0.6
    #   -> 1 tune trial should run in parallel.

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("CartPole-v1")
        .learners(num_learners=args.num_learners)
        .resources(
            num_learner_workers=args.num_learners,
            # How many GPUs does the local worker (driver) need? For most algos,
            # this is where the learning updates happen.
            # Set this to > 1 for multi-GPU learning.
            num_gpus=args.num_gpus,
            # How many GPUs does each RolloutWorker (`num_workers`) need?
            num_gpus_per_worker=args.num_gpus_per_worker,
        )
        # 4 tune trials altogether.
        .training(lr=tune.grid_search([0.005, 0.003, 0.001, 0.0001]))
    )

    stop = {
        TRAINING_ITERATION: args.stop_iters,
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    }

    # Note: The above GPU settings should also work in case you are not
    # running via ``Tuner.fit()``, but instead do:

    # >> from ray.rllib.algorithms.ppo import PPO
    # >> algo = PPO(config=config)
    # >> for _ in range(10):
    # >>     results = algo.train()
    # >>     print(results)

    run_rllib_example_script_experiment(base_config, args)
