"""Example demo'ing async gym vector envs, in which sub-envs have their own process.

Setting up env vectorization works through setting the `config.num_envs_per_env_runner`
value to > 1. However, by default the n sub-environments are stepped through
sequentially, rather than in parallel.

This script shows the effect of setting the `config.gym_env_vectorize_mode` from its
default value of "SYNC" (all sub envs are located in the same EnvRunner process)
to "ASYNC" (all sub envs in each EnvRunner get their own process).

This example:
    - shows, which config settings to change in order to switch from sub-envs being
    stepped in sequence to each sub-envs owning its own process (and compute resource)
    and thus the vector being stepped in parallel.
    - shows, how this setup can increase EnvRunner performance significantly, especially
    for heavier, slower environments.
    - uses an artificially slow CartPole-v1 environment for demonstration purposes.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack `

Use the `--vectorize-mode=BOTH` option to run both modes (SYNC and ASYNC)
through Tune at the same time and get a better comparison of the throughputs
achieved.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see results similar to the following in your console output
when using the

+--------------------------+------------+------------------------+------+
| Trial name               | status     | gym_env_vectorize_mode | iter |
|                          |            |                        |      |
|--------------------------+------------+------------------------+------+
| PPO_slow-env_6ddf4_00000 | TERMINATED | SYNC                   |    4 |
| PPO_slow-env_6ddf4_00001 | TERMINATED | ASYNC                  |    4 |
+--------------------------+------------+------------------------+------+
+------------------+----------------------+------------------------+
|   total time (s) |  episode_return_mean |   num_env_steps_sample |
|                  |                      |             d_lifetime |
|------------------+----------------------+------------------------+
|          60.8794 |                73.53 |                  16040 |
|          19.1203 |                73.86 |                  16037 |
+------------------+----------------------+------------------------+

You can see that the ASYNC mode, given that the env is sufficiently slow,
achieves much better results when using vectorization.

You should see no difference, however, when only using
`--num-envs-per-env-runner=1`.
"""
import time

import gymnasium as gym

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray import tune

parser = add_rllib_example_script_args(default_reward=60.0)
parser.set_defaults(
    enable_new_api_stack=True,
    env="CartPole-v1",
    num_envs_per_env_runner=6,
)
parser.add_argument(
    "--vectorize-mode",
    type=str,
    default="ASYNC",
    help="The value `gym.envs.registration.VectorizeMode` to use for env "
    "vectorization. SYNC steps through all sub-envs in sequence. ASYNC (default) "
    "parallelizes sub-envs through multiprocessing and can speed up EnvRunners "
    "significantly. Use the special value `BOTH` to run both ASYNC and SYNC through a "
    "Tune grid-search.",
)


class SlowEnv(gym.ObservationWrapper):
    def observation(self, observation):
        time.sleep(0.005)
        return observation


if __name__ == "__main__":
    args = parser.parse_args()

    if args.no_tune and args.vectorize_mode == "BOTH":
        raise ValueError(
            "Can't run this script with both --no-tune and --vectorize-mode=BOTH!"
        )

    # Wrap the env with the slowness wrapper.
    def _env_creator(cfg):
        return SlowEnv(gym.make(args.env, **cfg))

    tune.register_env("slow-env", _env_creator)

    if args.vectorize_mode == "BOTH" and args.no_tune:
        raise ValueError(
            "`--vectorize-mode=BOTH` and `--no-tune` not allowed in combination!"
        )

    base_config = (
        PPOConfig()
        .environment("slow-env")
        .env_runners(
            gym_env_vectorize_mode=(
                tune.grid_search(["SYNC", "ASYNC"])
                if args.vectorize_mode == "BOTH"
                else args.vectorize_mode
            ),
        )
    )

    results = run_rllib_example_script_experiment(base_config, args)

    # Compare the throughputs and assert that ASYNC is much faster than SYNC.
    if args.vectorize_mode == "BOTH":
        throughput_sync = (
            results[0].metrics["num_env_steps_sampled_lifetime"]
            / results[0].metrics["time_total_s"]
        )
        throughput_async = (
            results[1].metrics["num_env_steps_sampled_lifetime"]
            / results[1].metrics["time_total_s"]
        )
        assert throughput_async > throughput_sync
