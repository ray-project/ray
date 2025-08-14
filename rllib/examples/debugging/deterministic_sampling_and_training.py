"""Example of how to seed your experiment with the `config.debugging(seed=...)` option.

This example shows:
    - how to seed an experiment, both on the Learner and on the EnvRunner side.
    - that different experiments run with the exact same seed always yield the exact
    same results (use the `--as-test` option to enforce assertions on the results).
    Results checked range from EnvRunner stats, such as episode return, to Learner
    stats, such as losses and gradient averages.

Note that some algorithms, such as APPO which rely on asynchronous sampling in
combination with Ray network communication always behave stochastically, no matter
whether you set a seed or not. Therefore, make sure your `--algo` option is set to
a non-asynchronous algorithm, like "PPO" or "DQN".


How to run this script
----------------------
`python [script file name].py --seed 1234`

Use the `--num-learners=2` option to run with multiple Learner workers and, if GPUs
are available, place these workers on multiple GPUs.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should expect to see 2 experiments running and finishing in your console.
After the second experiment, you should see the confirmation that both experiments
yielded the exact same metrics.

+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|                             |            |                 |        |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_fb6d2_00000 | TERMINATED | 127.0.0.1:86298 |      3 |
+-----------------------------+------------+-----------------+--------+
+------------------+------------------------+------------------------+
|   total time (s) |    episode_return_mean |   num_env_steps_sample |
|                  |                        |             d_lifetime |
|------------------+------------------------+------------------------|
|           6.2416 |                  67.52 |                  12004 |
+------------------+------------------------+------------------------+

...

Determinism works! ok
"""
import ray
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    LEARNER_RESULTS,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    check,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env


parser = add_rllib_example_script_args(default_iters=3)
parser.set_defaults(
    # Test by default with more than one Env per EnvRunner.
    num_envs_per_env_runner=2,
)
parser.add_argument("--seed", type=int, default=42)


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentCartPole(config={"num_agents": args.num_agents}),
        )

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env" if args.num_agents > 0 else "CartPole-v1")
        # Make sure every environment gets a fixed seed.
        .debugging(seed=args.seed)
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    results1 = run_rllib_example_script_experiment(
        base_config,
        args,
        keep_ray_up=True,
        success_metric={ENV_RUNNER_RESULTS + "/" + EPISODE_RETURN_MEAN: 10.0},
    )
    results2 = run_rllib_example_script_experiment(
        base_config,
        args,
        keep_ray_up=True,
        success_metric={ENV_RUNNER_RESULTS + "/" + EPISODE_RETURN_MEAN: 10.0},
    )

    if args.as_test:
        results1 = results1.get_best_result().metrics
        results2 = results2.get_best_result().metrics

        # Test EnvRunner behaviors.
        check(
            results1[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN],
            results2[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN],
        )
        # As well as training behavior (minibatch sequence during SGD
        # iterations).
        for key in [
            # Losses and coefficients.
            "curr_kl_coeff",
            "vf_loss",
            "policy_loss",
            "entropy",
            "total_loss",
            "module_train_batch_size_mean",
            # Optimizer stuff.
            "gradients_default_optimizer_global_norm",
        ]:
            if args.num_agents > 0:
                for aid in range(args.num_agents):
                    check(
                        results1[LEARNER_RESULTS][f"p{aid}"][key],
                        results2[LEARNER_RESULTS][f"p{aid}"][key],
                    )
            else:
                check(
                    results1[LEARNER_RESULTS][DEFAULT_MODULE_ID][key],
                    results2[LEARNER_RESULTS][DEFAULT_MODULE_ID][key],
                )

        print("Determinism works! ok")

    ray.shutdown()
