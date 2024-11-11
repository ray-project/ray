"""Example demonstrating that RLlib can learn (at scale) in unstable environments.

This script uses the `CartPoleCrashing` environment, an adapted cartpole env whose
instability is configurable through setting the probability of a crash and/or stall
(sleep for a configurable amount of time) during `reset()` and/or `step()`.

RLlib has two major flags for EnvRunner fault tolerance, which can be independently
set to True:
1) `config.fault_tolerance(restart_failed_sub_environments=True)` causes only the
(gymnasium) environment object on an EnvRunner to be closed (try calling `close()` on
the faulty object), garbage collected, and finally recreated from scratch. Note that
during this process, the containing EnvRunner remaing up and running and sampling
simply continues after the env recycling. This is the lightest and fastest form of
fault tolerance and should be attempted first.
2) `config.fault_tolerance(restart_failed_env_runners=True)` causes the entire
EnvRunner (a Ray remote actor) to be restarted. This restart logically includes the
gymnasium environment, the RLModule, and all connector pipelines on the EnvRunner.
Use this option only if you face problems with the first option
(restart_failed_sub_environments=True), such as incomplete cleanups and memory leaks.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack

You can switch on the fault tolerant behavior (1) (restart_failed_sub_environments)
through the `--restart-failed-envs` flag. If this flag is not set, the script will
recreate the entire (faulty) EnvRunner.

You can switch on stalling (besides crashing) through the `--stall` command line flag.
If set, besides crashing on `reset()` and/or `step()`, there is also a chance of
stalling for a few seconds on each of these events.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see the following (or very similar) console output when running this script
with:
`--algo=PPO --stall --restart-failed-envs --stop-reward=450.0`
+---------------------+------------+----------------+--------+------------------+
| Trial name          | status     | loc            |   iter |   total time (s) |
|                     |            |                |        |                  |
|---------------------+------------+----------------+--------+------------------+
| PPO_env_ba39b_00000 | TERMINATED | 127.0.0.1:1401 |     22 |          133.497 |
+---------------------+------------+----------------+--------+------------------+
+------------------------+------------------------+------------------------+
|    episode_return_mean |   num_episodes_lifetim |   num_env_steps_traine |
|                        |                      e |             d_lifetime |
|------------------------+------------------------+------------------------|
|                 450.24 |                    542 |                  88628 |
+------------------------+------------------------+------------------------+

For APPO and testing restarting the entire EnvRunners, you could run the script with:
`--algo=APPO --stall --stop-reward=450.0`
+----------------------+------------+----------------+--------+------------------+
| Trial name           | status     | loc            |   iter |   total time (s) |
|                      |            |                |        |                  |
|----------------------+------------+----------------+--------+------------------+
| APPO_env_ba39b_00000 | TERMINATED | 127.0.0.1:4653 |     10 |          101.531 |
+----------------------+------------+----------------+--------+------------------+
+------------------------+------------------------+------------------------+
|    episode_return_mean |   num_episodes_lifetim |   num_env_steps_traine |
|                        |                      e |             d_lifetime |
|------------------------+------------------------+------------------------|
|                 478.85 |                   2546 |                 321500 |
+------------------------+------------------------+------------------------+
"""
from gymnasium.wrappers import TimeLimit

from ray import tune
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.envs.classes.cartpole_crashing import (
    CartPoleCrashing,
    MultiAgentCartPoleCrashing,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=450.0,
    default_timesteps=2000000,
)
parser.set_defaults(
    enable_new_api_stack=True,
    num_env_runners=4,
    num_envs_per_env_runner=2,
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
parser.add_argument(
    "--stall",
    action="store_true",
    help="Whether to also stall the env from time to time",
)
parser.add_argument(
    "--restart-failed-envs",
    action="store_true",
    help="Whether to restart a failed environment (vs restarting the entire "
    "EnvRunner).",
)


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    if args.num_agents > 0:
        tune.register_env("env", lambda cfg: MultiAgentCartPoleCrashing(cfg))
    else:
        tune.register_env(
            "env",
            lambda cfg: TimeLimit(CartPoleCrashing(cfg), max_episode_steps=500),
        )

    base_config = (
        tune.registry.get_trainable_cls(args.algo)
        .get_default_config()
        .environment(
            "env",
            env_config={
                "num_agents": args.num_agents,
                # Probability to crash during step().
                "p_crash": 0.0001,
                # Probability to crash during reset().
                "p_crash_reset": 0.001,
                "crash_on_worker_indices": [1, 2],
                "init_time_s": 2.0,
                # Probability to stall during step().
                "p_stall": 0.0005,
                # Probability to stall during reset().
                "p_stall_reset": 0.001,
                # Stall from 2 to 5sec (or 0.0 if --stall not set).
                "stall_time_sec": (2, 5) if args.stall else 0.0,
                # EnvRunner indices to stall on.
                "stall_on_worker_indices": [2, 3],
            },
        )
        # Switch on resiliency.
        .fault_tolerance(
            # Recreate any failed EnvRunners.
            restart_failed_env_runners=True,
            # Restart any failed environment (w/o recreating the EnvRunner). Note that
            # this is the much faster option.
            restart_failed_sub_environments=args.restart_failed_envs,
        )
    )

    # Use more stabilizing hyperparams for APPO.
    if args.algo == "APPO":
        base_config.training(
            grad_clip=40.0,
            entropy_coeff=0.0,
            vf_loss_coeff=0.05,
        )
        base_config.rl_module(
            model_config=DefaultModelConfig(vf_share_layers=True),
        )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    run_rllib_example_script_experiment(base_config, args=args)
