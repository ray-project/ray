"""Example of customizing the evaluation procedure for an RLlib Algorithm.

Note, that you should only choose to provide a custom eval function, in case the already
built-in eval options are not sufficient. Normally, though, RLlib's eval utilities
that come with each Algorithm are enough to properly evaluate the learning progress
of your Algorithm.

This script uses the SimpleCorridor environment, a simple 1D gridworld, in which
the agent can only walk left (action=0) or right (action=1). The goal state is located
at the end of the (1D) corridor. The env exposes an API to change the length of the
corridor on-the-fly. We use this API here to extend the size of the corridor for the
evaluation runs.

For demonstration purposes only, we define a simple custom evaluation method that does
the following:
- It changes the corridor length of all environments used on the evaluation EnvRunners.
- It runs a defined number of episodes for evaluation purposes.
- It collects the metrics from those runs, summarizes these metrics and returns them.


How to run this script
----------------------
`python [script file name].py

You can switch off custom evaluation (and use RLlib's default evaluation procedure)
with the `--no-custom-eval` flag.

You can switch on parallel evaluation to training using the
`--evaluation-parallel-to-training` flag. See this example script here:
https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/evaluation_parallel_to_training.py  # noqa
for more details on running evaluation parallel to training.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see the following (or very similar) console output when running this script.
Note that for each iteration, due to the definition of our custom evaluation function,
we run 3 evaluation rounds per single training round.

...
Training iteration 1 -> evaluation round 0
Training iteration 1 -> evaluation round 1
Training iteration 1 -> evaluation round 2
...
...
+--------------------------------+------------+-----------------+--------+
| Trial name                     | status     | loc             |   iter |
|--------------------------------+------------+-----------------+--------+
| PPO_SimpleCorridor_06582_00000 | TERMINATED | 127.0.0.1:69905 |      4 |
+--------------------------------+------------+-----------------+--------+
+------------------+-------+----------+--------------------+
|   total time (s) |    ts |   reward |   episode_len_mean |
|------------------+-------+----------+--------------------|
|          26.1973 | 16000 | 0.872034 |            13.7966 |
+------------------+-------+----------+--------------------+
"""
from typing import Tuple

from ray.tune.result import TRAINING_ITERATION
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.env_runner_group import EnvRunnerGroup
from ray.rllib.examples.envs.classes.simple_corridor import SimpleCorridor
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EVALUATION_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.typing import ResultDict
from ray.tune.registry import get_trainable_cls


parser = add_rllib_example_script_args(
    default_iters=50,
    default_reward=0.7,
    default_timesteps=50000,
)
parser.add_argument("--no-custom-eval", action="store_true")
parser.add_argument("--corridor-length-training", type=int, default=10)
parser.add_argument("--corridor-length-eval-worker-1", type=int, default=20)
parser.add_argument("--corridor-length-eval-worker-2", type=int, default=30)


def custom_eval_function(
    algorithm: Algorithm,
    eval_workers: EnvRunnerGroup,
) -> Tuple[ResultDict, int, int]:
    """Example of a custom evaluation function.

    Args:
        algorithm: Algorithm class to evaluate.
        eval_workers: Evaluation EnvRunnerGroup.

    Returns:
        metrics: Evaluation metrics dict.
    """
    # Set different env settings for each (eval) EnvRunner. Here we use the EnvRunner's
    # `worker_index` property to figure out the actual length.
    # Loop through all workers and all sub-envs (gym.Env) on each worker and call the
    # `set_corridor_length` method on these.
    eval_workers.foreach_env_runner(
        func=lambda worker: (
            env.unwrapped.set_corridor_length(
                args.corridor_length_eval_worker_1
                if worker.worker_index == 1
                else args.corridor_length_eval_worker_2
            )
            for env in worker.env.unwrapped.envs
        )
    )

    # Collect metrics results collected by eval workers in this list for later
    # processing.
    env_runner_metrics = []
    sampled_episodes = []
    # For demonstration purposes, run through some number of evaluation
    # rounds within this one call. Note that this function is called once per
    # training iteration (`Algorithm.train()` call) OR once per `Algorithm.evaluate()`
    # (which can be called manually by the user).
    for i in range(3):
        print(f"Training iteration {algorithm.iteration} -> evaluation round {i}")
        # Sample episodes from the EnvRunners AND have them return only the thus
        # collected metrics.
        episodes_and_metrics_all_env_runners = eval_workers.foreach_env_runner(
            # Return only the metrics, NOT the sampled episodes (we don't need them
            # anymore).
            func=lambda worker: (worker.sample(), worker.get_metrics()),
            local_env_runner=False,
        )
        sampled_episodes.extend(
            eps
            for eps_and_mtrcs in episodes_and_metrics_all_env_runners
            for eps in eps_and_mtrcs[0]
        )
        env_runner_metrics.extend(
            eps_and_mtrcs[1] for eps_and_mtrcs in episodes_and_metrics_all_env_runners
        )

    # You can compute metrics from the episodes manually, or use the Algorithm's
    # convenient MetricsLogger to store all evaluation metrics inside the main
    # algo.
    algorithm.metrics.aggregate(
        env_runner_metrics, key=(EVALUATION_RESULTS, ENV_RUNNER_RESULTS)
    )
    eval_results = algorithm.metrics.peek((EVALUATION_RESULTS, ENV_RUNNER_RESULTS))
    # Alternatively, you could manually reduce over the n returned `env_runner_metrics`
    # dicts, but this would be much harder as you might not know, which metrics
    # to sum up, which ones to average over, etc..

    # Compute env and agent steps from sampled episodes.
    env_steps = sum(eps.env_steps() for eps in sampled_episodes)
    agent_steps = sum(eps.agent_steps() for eps in sampled_episodes)

    return eval_results, env_steps, agent_steps


if __name__ == "__main__":
    args = parser.parse_args()
    args.local_mode = True
    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # For training, we use a corridor length of n. For evaluation, we use different
        # values, depending on the eval worker index (1 or 2).
        .environment(
            SimpleCorridor,
            env_config={"corridor_length": args.corridor_length_training},
        )
        .env_runners(create_env_on_local_worker=True)
        .evaluation(
            # Do we use the custom eval function defined above?
            custom_evaluation_function=(
                None if args.no_custom_eval else custom_eval_function
            ),
            # Number of eval EnvRunners to use.
            evaluation_num_env_runners=2,
            # Enable evaluation, once per training iteration.
            evaluation_interval=1,
            # Run 10 episodes each time evaluation runs (OR "auto" if parallel to
            # training).
            evaluation_duration="auto" if args.evaluation_parallel_to_training else 10,
            # Evaluate parallelly to training?
            evaluation_parallel_to_training=args.evaluation_parallel_to_training,
            # Override the env settings for the eval workers.
            # Note, though, that this setting here is only used in case --no-custom-eval
            # is set, b/c in case the custom eval function IS used, we override the
            # length of the eval environments in that custom function, so this setting
            # here is simply ignored.
            evaluation_config=AlgorithmConfig.overrides(
                env_config={"corridor_length": args.corridor_length_training * 2},
                # TODO (sven): Add support for window=float(inf) and reduce=mean for
                #  evaluation episode_return_mean reductions (identical to old stack
                #  behavior, which does NOT use a window (100 by default) to reduce
                #  eval episode returns.
                metrics_num_episodes_for_smoothing=5,
            ),
        )
    )

    stop = {
        TRAINING_ITERATION: args.stop_iters,
        f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": (
            args.stop_reward
        ),
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
    }

    run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        success_metric={
            f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": (
                args.stop_reward
            ),
        },
    )
