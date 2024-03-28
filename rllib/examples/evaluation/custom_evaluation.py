"""Example of customizing the evaluation procedure for an RLlib algorithm.

Note, that you should only choose to provide a custom eval function, in case the already
built-in eval options are not sufficient. Normally, though, RLlib's eval utilities
that come with each Algorithm are enough to properly evaluate the learning progress
of your Algorithm.

This script uses the SimpleCorridor environment, a simple 1D gridworld, in which
the agent can only walk left (action=0) or right (action=1). The goal is at the end of
the (1D) corridor. The env exposes an API to change the length of the corridor
on-the-fly. We use this API here to extend the size of the corridor for the evaluation
runs.

We define a custom evaluation method that does the following:
- It changes the corridor length of all environments used on the evaluation EnvRunners.
- It runs a defined number of episodes for evaluation purposes.
- It collects the metrics from those runs, summarizes these metrics and returns them.

"""
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.evaluation.metrics import summarize_episodes
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.examples.env.simple_corridor import SimpleCorridor
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.typing import ResultDict
from ray.tune.registry import get_trainable_cls


parser = add_rllib_example_script_args(
    default_iters=50, default_reward=0.7, default_timesteps=50000
)
parser.add_argument("--evaluation-parallel-to-training", action="store_true")
parser.add_argument("--no-custom-eval", action="store_true")
parser.add_argument("--corridor-length-training", type=int, default=10)
parser.add_argument("--corridor-length-eval-worker-1", type=int, default=20)
parser.add_argument("--corridor-length-eval-worker-2", type=int, default=30)


def custom_eval_function(algorithm: Algorithm, eval_workers: WorkerSet) -> ResultDict:
    """Example of a custom evaluation function.

    Args:
        algorithm: Algorithm class to evaluate.
        eval_workers: Evaluation WorkerSet.

    Returns:
        metrics: Evaluation metrics dict.
    """
    # Set different env settings for each (eval) EnvRunner. Here we use the EnvRunner's
    # `worker_index` property to figure out the actual length.
    # Loop through all workers and all sub-envs (gym.Env) on each worker and call the
    # `set_corridor_length` method on these.
    eval_workers.foreach_worker(
        func=lambda worker: (
            env.set_corridor_length(
                args.corridor_length_eval_worker_1
                if worker.worker_index == 1
                else args.corridor_length_eval_worker_2
            )
            for env in worker.env.envs
        )
    )

    # Collect metrics results collected by eval workers in this list for later
    # processing.
    rollout_metrics = []

    # For demonstration purposes, run through some arbitrary number of evaluation
    # round within this one call. Note that this function is called once per
    # training iteration (`Algorithm.train()` call) OR once per `Algorithm.evaluate()`
    # (which may be called manually by the user).
    for i in range(3):
        print(f"Training iteration {algorithm.iteration} -> evaluation round {i}")
        # Sample episodes from the EnvRunners AND have them return only the thus
        # collected metrics.
        metrics_all_workers = eval_workers.foreach_worker(
            # Return only the metrics, NOT the sampled episodes (we don't need them
            # anymore).
            func=lambda worker: (worker.sample(), worker.get_metrics())[1],
            local_worker=False,
        )
        for metrics_per_worker in metrics_all_workers:
            rollout_metrics.extend(metrics_per_worker)

    # You can compute metrics from the episodes manually, or use the
    # convenient `summarize_episodes()` utility:
    eval_results = summarize_episodes(rollout_metrics)

    return eval_results


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # For training, we use a corridor length of n. For evaluation, we use different
        # values, depending on the eval worker index (1 or 2).
        .environment(
            SimpleCorridor,
            env_config={"corridor_length": args.corridor_length_training},
        )
        .evaluation(
            # Do we use the custom eval function defined above?
            custom_evaluation_function=(
                None if args.no_custom_eval else custom_eval_function
            ),
            # Number of eval EnvRunners to use.
            evaluation_num_workers=2,
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
            ),
        )
    )

    stop = {
        "training_iteration": args.stop_iters,
        "evaluation/sampler_results/episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        success_metric="evaluation/sampler_results/episode_reward_mean",
    )
